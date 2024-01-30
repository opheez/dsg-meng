using FASTER.darq;
using Grpc.Core;

namespace SimpleWorkflowBench;


public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase
{
    public override Task<ExecuteWorkflowResult> ExecuteWorkflow(ExecuteWorkflowRequest request, ServerCallContext context)
    {
        return base.ExecuteWorkflow(request, context);
    }
}