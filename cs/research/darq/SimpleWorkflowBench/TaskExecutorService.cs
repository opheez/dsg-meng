using Google.Protobuf;
using Grpc.Core;

namespace SimpleWorkflowBench;

public class TaskExecutorService : TaskExecutor.TaskExecutorBase
{
    private ThreadLocal<Random> random = new(() => new Random());
    public override async Task<ExecuteTaskResponse> ExecuteTask(ExecuteTaskRequest request, ServerCallContext context)
    {
        await Task.Delay(request.DurationMilli);
        var resultArray = new byte[1 << 15];
        random.Value.NextBytes(resultArray);
        return new ExecuteTaskResponse
        {
            WorkflowId = request.WorkflowId,
            TaskId = request.TaskId,
            Output = ByteString.CopyFrom(resultArray)
        };
    }
}