using System.Collections.Concurrent;
using darq;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;

namespace SimpleWorkflowBench;

internal struct WorkflowStatusDarqEntry : ILogEnqueueEntry
{
    internal long workflowId;
    internal int numTasksLeft;
    internal ByteString result;

    public int SerializedLength => sizeof(long) + 2 * sizeof(int) + (result?.Length ?? 0);

    public void SerializeTo(Span<byte> dest)
    {
        unsafe
        {
            fixed (byte* d = dest)
            {
                var head = d;
                *(long*)head = workflowId;
                head += sizeof(long);
                *(int*)head = numTasksLeft;
                head += sizeof(int);
                *(int*)head = result?.Length ?? 0;
                head += sizeof(int);
                result?.Span.CopyTo(new Span<byte>(head, dest.Length  - (int)(head - d)));
            }
        }
    }
}

internal struct StartActivityDarqEntry : ILogEnqueueEntry
{
    internal long workflowId;
    internal int taskId, durationMilli;
    internal ByteString taskInput;

    public int SerializedLength => sizeof(long) + 3 * sizeof(int) + taskInput.Length;

    public void SerializeTo(Span<byte> dest)
    {
        unsafe
        {
            fixed (byte* d = dest)
            {
                var head = d;
                *(long*)head = workflowId;
                head += sizeof(long);
                *(int*)head = taskId;
                head += sizeof(int);
                *(int*)head = durationMilli;
                head += sizeof(int);
                *(int*)head = taskInput.Length;
                head += sizeof(int);
                taskInput.Span.CopyTo(new Span<byte>(head, dest.Length  - (int)(head - d)));
            }
        }
    }
}

public class WorkflowHandle
{
    internal long workflowId;
    internal int remainingTasks;
    internal byte[] result;
    internal TaskCompletionSource<ExecuteWorkflowResult> tcs = new();

    public WorkflowHandle(long workflowId, int remainingTasks)
    {
        this.workflowId = workflowId;
        this.remainingTasks = remainingTasks;
    }
}

public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase, IDarqProcessor, IDisposable
{
    private Darq backend;
    private readonly DarqBackgroundTask _backgroundTask;
    private readonly DarqBackgroundWorkerPool workerPool;
    private readonly ManualResetEventSlim terminationStart, terminationComplete;
    private Thread refreshThread, processingThread;
    private ColocatedDarqProcessorClient<RwLatchVersionScheme> processorClient;

    private ConcurrentDictionary<long, WorkflowHandle> startedWorkflows;
    private IDarqProcessorClientCapabilities capabilities;

    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private List<GrpcChannel> workers;
    private int nextWorker = 0;

    public WorkflowOrchestratorService(Darq darq, List<GrpcChannel> executors, DarqBackgroundWorkerPool workerPool)
    {
        backend = darq;
        workers = executors;
        // no need to supply a cluster because we don't use inter-DARQ messaging in this use case
        _backgroundTask = new DarqBackgroundTask(backend, workerPool, null);
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new ManualResetEventSlim();
        this.workerPool = workerPool;
        backend.ConnectToCluster();
        
        _backgroundTask.StopProcessing();

        refreshThread = new Thread(() =>
        {
            while (!terminationStart.IsSet)
                backend.Refresh();
            terminationComplete.Set();
        });
        refreshThread.Start();

        processorClient = new ColocatedDarqProcessorClient<RwLatchVersionScheme>(backend);
        processingThread = new Thread(() =>
        {
            processorClient.StartProcessing(this);
        });
        processingThread.Start();
        // TODO(Tianyu): Hacky
        // spin until we are sure that we have started 
        while (capabilities == null) {}
    }

    public Darq GetBackend() => backend;

    public void Dispose()
    {
        terminationStart.Set();
        // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
        backend.ForceCheckpoint();
        Thread.Sleep(1000);
        _backgroundTask.StopProcessing();
        _backgroundTask.Dispose();
        processorClient.StopProcessingAsync().GetAwaiter().GetResult();
        processorClient.Dispose();
        terminationComplete.Wait();
        refreshThread.Join();
        processingThread.Join();
    }

    public override async Task<ExecuteWorkflowResult> ExecuteWorkflow(ExecuteWorkflowRequest request,
        ServerCallContext context)
    {
        var handle = new WorkflowHandle(request.WorkflowId, request.Depth);
        var actualHandle = startedWorkflows.GetOrAdd(request.WorkflowId, handle);
        if (actualHandle == handle)
        {
            // This handle was created by this thread, which gives us the ability to go ahead and start the workflow
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.AddRecoveryMessage(new WorkflowStatusDarqEntry
            {
                workflowId = request.WorkflowId,
                numTasksLeft = request.Depth,
            });
            requestBuilder.AddSelfMessage(new StartActivityDarqEntry
            {
                workflowId = request.WorkflowId,
                taskId = 0,
                durationMilli = 10,
                taskInput = request.Input
            });
            await capabilities.Step(requestBuilder.FinishStep());
            Console.WriteLine($"Workflow {request.WorkflowId} started");
            stepRequestPool.Return(stepRequest);
        }
        backend.EndAction();
        
        // Otherwise, some other concurrent thread will start this workflow -- simply forward the result
        var result = await handle.tcs.Task;
        Console.WriteLine($"Workflow {request.WorkflowId} finished");
        return result;
    }

    private unsafe ExecuteTaskRequest ComposeActivityRequest(DarqMessage m)
    {
        var result = new ExecuteTaskRequest();
        fixed (byte* d = m.GetMessageBody())
        {
            var head = d;
            result.WorkflowId = *(long*) head;
            head += sizeof(long);
            result.TaskId = *(int*) head;
            head += sizeof(int);
            var size = *(int*)head;
            head += sizeof(int);
            result.Input = ByteString.CopyFrom(new ReadOnlySpan<byte>(head, size));
        }
        
        m.Dispose();
        return result;
    }

    public async Task ExecuteActivity(ExecuteTaskRequest r, long lsn)
    {
        // round-robin executor to execute the next request
        Console.WriteLine($"Workflow {r.WorkflowId} starting an activity");
        
        var worker = Interlocked.Increment(ref nextWorker) % workers.Count;
        var session = backend.DetachFromWorker();
        var client = new TaskExecutor.TaskExecutorClient(
            workers[worker].Intercept(new DprClientInterceptor(session)));

        var response = await client.ExecuteTaskAsync(r);
        Console.WriteLine($"Workflow {r.WorkflowId} activity completed");

        if (!backend.TryMergeAndStartAction(session)) return;
        
        var workflowHandle = startedWorkflows[r.WorkflowId];
        var decremented = Interlocked.Decrement(ref workflowHandle.remainingTasks);
        var stepRequest = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(stepRequest);
        requestBuilder.MarkMessageConsumed(lsn);
        if (decremented == 0)
        {
            requestBuilder.AddRecoveryMessage(new WorkflowStatusDarqEntry
            {
                workflowId = workflowHandle.workflowId,
                numTasksLeft = 0,
                result = response.Output
            });
            workflowHandle.tcs.SetResult(new ExecuteWorkflowResult
            {
                Ok = true,
                Result = response.Output
            });
        }
        else
        {
            requestBuilder.AddRecoveryMessage(new WorkflowStatusDarqEntry
            {
                workflowId = workflowHandle.workflowId,
                // TODO(TIanyu): will no longer be correct if we have more than one task from a workflow at the same time
                numTasksLeft = decremented,
            });
            requestBuilder.AddSelfMessage(new StartActivityDarqEntry
            {
                workflowId = workflowHandle.workflowId,
                taskId = r.TaskId + 1,
                durationMilli = 10,
                taskInput = response.Output
            });
            if (decremented == 2)
                await session.SpeculationBarrier(backend.GetDprFinder());
        }

        await capabilities.Step(requestBuilder.FinishStep());
        stepRequestPool.Return(stepRequest);
        backend.EndAction();
    }

    public unsafe void ApplyWorkflowStatusUpdate(DarqMessage m)
    {
        backend.StartLocalAction();
        fixed (byte* d = m.GetMessageBody())
        {
            var head = d;
            var workflowId = *(long*)head;
            head += sizeof(long);
            var numTasksLeft = *(int*)head;
            head += sizeof(int);
            var resultLength = *(int*)head;
            head += sizeof(int);
            if (startedWorkflows.TryGetValue(workflowId, out var value))
            {
                value.remainingTasks = numTasksLeft;
                if (resultLength != 0) value.result = new Span<byte>(head, resultLength).ToArray();
            }
            else
            {
                var newHandle = new WorkflowHandle(workflowId, numTasksLeft);
                if (resultLength != 0) newHandle.result = new Span<byte>(head, resultLength).ToArray();
                startedWorkflows.TryAdd(workflowId, newHandle);
            }
        }
        m.Dispose();
        backend.EndAction();
    }

    public bool ProcessMessage(DarqMessage m)
    {
        switch (m.GetMessageType())
        {
            case DarqMessageType.IN:
                var request = ComposeActivityRequest(m);
                var lsn = m.GetLsn();
                workerPool.AddWork(() => ExecuteActivity(request, lsn));
                break;
            case DarqMessageType.RECOVERY:
                ApplyWorkflowStatusUpdate(m);
                break;
            default:
                throw new NotImplementedException();
        }
        m.Dispose();
        return true;
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities)
    {
        if (startedWorkflows != null)
        {
            foreach (var handle in startedWorkflows.Values)
                handle.tcs.TrySetCanceled();
        }
        startedWorkflows = new ConcurrentDictionary<long, WorkflowHandle>();
        this.capabilities = capabilities;

    }
}