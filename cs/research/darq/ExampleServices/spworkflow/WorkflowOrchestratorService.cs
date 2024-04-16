using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using darq;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace dse.services;

public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase
{
    private OrchestratorBackgroundProcessingService backend;
    private ILogger<WorkflowOrchestratorService> logger;
    public WorkflowOrchestratorService(OrchestratorBackgroundProcessingService backend, ILogger<WorkflowOrchestratorService> logger)
    {
        this.backend = backend;
        this.logger = logger;
    }
    
    public override Task<ExecuteWorkflowResult> ExecuteWorkflow(ExecuteWorkflowRequest request,
        ServerCallContext context)
    {
        logger.LogInformation(
            $"Received execute workflow request, id of {request.WorkflowId}, class id of {request.WorkflowClassId}, request string of {Encoding.UTF8.GetString(request.Input.Span)}");
        return backend.CreateWorkflow(request);
    }
}