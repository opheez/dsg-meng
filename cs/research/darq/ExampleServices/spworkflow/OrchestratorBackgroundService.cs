using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace dse.services;

public class OrchestratorBackgroundProcessingService : BackgroundService, IDarqProcessor
{
    private Darq backend;
    private ColocatedDarqProcessorClient processorClient;
    private Dictionary<int, WorkflowFactory> workflowFactories;

    private ConcurrentDictionary<long, IWorkflowStateMachine> startedWorkflows = new();
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private ILogger<OrchestratorBackgroundProcessingService> logger;
    private CancellationTokenSource cts;

    public delegate IWorkflowStateMachine WorkflowFactory(StateObject obj, ReadOnlySpan<byte> input);
    
     public OrchestratorBackgroundProcessingService(Darq darq, Dictionary<int, WorkflowFactory> workflowFactories, ILogger<OrchestratorBackgroundProcessingService> logger)
    {
        backend = darq;
        processorClient = new ColocatedDarqProcessorClient(backend);
        this.workflowFactories = workflowFactories;
        this.logger = logger;
    }
     
     protected override async Task ExecuteAsync(CancellationToken stoppingToken)
     {
         backend.ConnectToCluster(out _);
         await processorClient.StartProcessingAsync(this, stoppingToken);
         backend.ForceCheckpoint(spin: true);
         processorClient.Dispose();
     }

    public async Task<ExecuteWorkflowResult> CreateWorkflow(ExecuteWorkflowRequest request)
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
            var success = backend.Enqueue(requestBuilder.FinishStep(), -1, 0);
            Debug.Assert(success);
            logger.LogInformation($"Workflow {request.WorkflowId} started");
            stepRequestPool.Return(stepRequest);
        }
        return await GetWorkflowResultAsync(request.WorkflowId, actualHandler);
    }

    private async Task<ExecuteWorkflowResult> GetWorkflowResultAsync(long workflowId, IWorkflowStateMachine workflow)
    {
        while (true)
        {
            var s = backend.DetachFromWorkerAndPauseAction();
            try
            {
                var result = await workflow.GetResult(cts.Token);
                if (backend.TryMergeAndStartAction(s)) return result;
            }
            catch (TaskCanceledException)
            {
            }

            // Otherwise, there has been a rollback, should retry with a new handle, if any
            while (!startedWorkflows.TryGetValue(workflowId, out workflow))
                await Task.Yield();
            backend.StartLocalAction();
        }
    }

    public bool ProcessMessage(DarqMessage m)
    {
        var workflowId = BitConverter.ToInt64(m.GetMessageBody());
        if (workflowId < 0)
        {         
            logger.LogInformation($"Replaying Workflow creation for id {-workflowId}");
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
        cts?.Cancel();
        startedWorkflows = new ConcurrentDictionary<long, IWorkflowStateMachine>();
        this.capabilities = capabilities;
        cts = new CancellationTokenSource();
    }
}