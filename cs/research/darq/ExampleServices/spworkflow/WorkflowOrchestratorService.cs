using System.Collections.Concurrent;
using System.Diagnostics;
using darq;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;

namespace SimpleWorkflowBench;
public interface IWorkflowStateMachine
{
    public void ProcessMessage(DarqMessage m);
    
    public void OnRestart(IDarqProcessorClientCapabilities capabilities, StateObject stateObject);
    
    Task<ExecuteWorkflowResult> GetResult(CancellationToken token);
}


public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase, IDarqProcessor, IDisposable
{
    private Darq backend;
    private readonly DarqBackgroundTask _backgroundTask;
    private readonly ManualResetEventSlim terminationStart, terminationComplete;
    private Thread refreshThread, processingThread;
    private ColocatedDarqProcessorClient processorClient;
    private Dictionary<int, WorkflowFactory> workflowFactories;

    private ConcurrentDictionary<long, IWorkflowStateMachine> startedWorkflows;
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private CancellationTokenSource cancellationSource;

    public delegate IWorkflowStateMachine WorkflowFactory(StateObject obj, ReadOnlySpan<byte> input);
    
    public WorkflowOrchestratorService(Darq darq, DarqBackgroundWorkerPool workerPool, Dictionary<int, WorkflowFactory> workflowFactories)
    {
        backend = darq;
        // no need to supply a cluster/messaging utils because we don't use inter-DARQ messaging in this use case
        _backgroundTask = new DarqBackgroundTask(backend, workerPool, null);
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new ManualResetEventSlim();
        backend.ConnectToCluster(out _);

        _backgroundTask.BeginProcessing();

        // TODO(Tianyu): Consider moving these onto the background worker pool as well
        refreshThread = new Thread(() =>
        {
            while (!terminationStart.IsSet)
                backend.Refresh();
            terminationComplete.Set();
        });
        refreshThread.Start();

        processorClient = new ColocatedDarqProcessorClient(backend);
        processingThread = new Thread(() => { processorClient.StartProcessing(this); });
        processingThread.Start();
        this.workflowFactories = workflowFactories;
        // TODO(Tianyu): Hacky
        // spin until we are sure that we have started 
        while (capabilities == null)
            Thread.Yield();
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
        var workflowHandler = workflowFactories[request.WorkflowClassId](backend, request.Input.Span);
        workflowHandler.OnRestart(capabilities, backend);
        var actualHandler = startedWorkflows.GetOrAdd(request.WorkflowId, workflowHandler);
        if (actualHandler == workflowHandler)
        {
            // This handle was created by this thread, which gives us the ability to go ahead and start the workflow
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.AddRecoveryMessage(-request.WorkflowId, request.ToByteArray());
            // Start the workflow by giving it an initial message
            requestBuilder.AddSelfMessage(request.WorkflowId, Span<byte>.Empty);
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
            try
            {
                var result = await workflow.GetResult(cancellationSource.Token);
                if (backend.TryMergeAndStartAction(s)) return result;
            }
            catch (TaskCanceledException) {}

            // Otherwise, there has been a rollback, should retry with a new handle, if any
            while (!startedWorkflows.TryGetValue(workflowId, out workflow))
                await Task.Yield();
        }
    }

    public bool ProcessMessage(DarqMessage m)
    {
        var workflowId = BitConverter.ToInt64(m.GetMessageBody());
        if (workflowId < 0)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.RECOVERY);
            var request = ExecuteWorkflowRequest.Parser.ParseFrom(m.GetMessageBody());
            var workflow = workflowFactories[request.WorkflowClassId](backend, request.Input.Span);
            workflow.OnRestart(capabilities, backend);
            var ok = startedWorkflows.TryAdd(-workflowId, workflow);
            Debug.Assert(ok);
            return true;
        }
        startedWorkflows[workflowId].ProcessMessage(m);
        return true;
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities)
    {
        cancellationSource?.Cancel();
        startedWorkflows = new ConcurrentDictionary<long, IWorkflowStateMachine>();
        cancellationSource = new CancellationTokenSource();
        this.capabilities = capabilities;
    }
}