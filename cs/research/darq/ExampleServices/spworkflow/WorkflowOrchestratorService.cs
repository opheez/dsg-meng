using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Runtime.InteropServices.JavaScript;
using Azure.Core;
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
public interface IWorkflowStateMachine : IDarqProcessor
{
    Task<ExecuteWorkflowResult> GetResult(CancellationToken token);
}

public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase, IDarqProcessor, IDisposable
{
    private Darq backend;
    private readonly DarqBackgroundTask _backgroundTask;
    private readonly DarqBackgroundWorkerPool workerPool;
    private readonly ManualResetEventSlim terminationStart, terminationComplete;
    private Thread refreshThread, processingThread;
    private ColocatedDarqProcessorClient<RwLatchVersionScheme> processorClient;
    private Dictionary<int, Func<DprWorker, IWorkflowStateMachine>> workflowFactories;

    private ConcurrentDictionary<long, IWorkflowStateMachine> startedWorkflows;
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());


    public WorkflowOrchestratorService(Darq darq, DarqBackgroundWorkerPool workerPool)
    {
        backend = darq;
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
        processingThread = new Thread(() => { processorClient.StartProcessing(this); });
        processingThread.Start();
        // TODO(Tianyu): Hacky
        // spin until we are sure that we have started 
        while (capabilities == null)
        {
        }
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

    public void BindWorkflowHandler(int classId, Func<DprWorker, IWorkflowStateMachine> factory)
    {
        workflowFactories[classId] = factory;
    }

    public override async Task<ExecuteWorkflowResult> ExecuteWorkflow(ExecuteWorkflowRequest request,
        ServerCallContext context)
    {
        var workflowHandler = workflowFactories[request.WorkflowClassId](backend);
        workflowHandler.OnRestart(capabilities);
        var actualHandler = startedWorkflows.GetOrAdd(request.WorkflowId, workflowHandler);
        if (actualHandler == workflowHandler)
        {
            // This handle was created by this thread, which gives us the ability to go ahead and start the workflow
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            unsafe
            {
                var workflowClassId = request.WorkflowClassId;
                requestBuilder.AddRecoveryMessage(-request.WorkflowId,
                    new ReadOnlySpan<byte>(&workflowClassId, sizeof(int)));
            }

            requestBuilder.AddSelfMessage(request.WorkflowId, request.Input.Span);
            await capabilities.Step(requestBuilder.FinishStep());
            Console.WriteLine($"Workflow {request.WorkflowId} started");
            stepRequestPool.Return(stepRequest);
        }

        return await GetWorkflowResultAsync(request.WorkflowId, actualHandler);
    }

    private async Task<ExecuteWorkflowResult> GetWorkflowResultAsync(long workflowId, IWorkflowStateMachine workflow)
    {
        while (true)
        {
            var s = backend.DetachFromWorker();
            // TODO(Tianyu): Fix later
            var result = await workflow.GetResult(default);
            if (backend.TryMergeAndStartAction(s)) return result;
            // Otherwise, there has been a rollback, should retry with a new handle, if any
            while (!startedWorkflows.TryGetValue(workflowId, out workflow))
                await Task.Yield();
        }
    }

    public bool ProcessMessage(DarqMessage m)
    {
        var partitionId = BitConverter.ToInt64(m.GetMessageBody());
        if (partitionId < 0)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.RECOVERY);
            var classId = BitConverter.ToInt32(m.GetMessageBody()[sizeof(long)..]);
            var workflow = workflowFactories[classId](backend);
            workflow.OnRestart(capabilities);
            var ok = startedWorkflows.TryAdd(-partitionId, workflow);
            Debug.Assert(ok);
            return true;
        }
        return startedWorkflows[partitionId].ProcessMessage(m);
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities)
    {
        if (startedWorkflows != null)
        {
            foreach (var handle in startedWorkflows.Values)
            {
                // TODO(Tianyu): Cancel all current waits
            }
        }

        startedWorkflows = new ConcurrentDictionary<long, IWorkflowStateMachine>();
        this.capabilities = capabilities;
    }
}