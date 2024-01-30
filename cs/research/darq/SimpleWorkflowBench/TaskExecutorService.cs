using Grpc.Core;

namespace SimpleWorkflowBench;

// TODO(Tianyu): Stateless for now 
public class TaskExecutorService : TaskExecutor.TaskExecutorBase
{
    public override async Task<ExecuteTaskResponse> ExecuteTask(ExecuteTaskRequest request, ServerCallContext context)
    {
        await Task.Delay(request.DurationMilli);
        return new ExecuteTaskResponse
        {
            Ok = true,
            Padding = null
        };
    }
}