// See https://aka.ms/new-console-template for more information

using CommandLine;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using SimpleWorkflowBench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('n', "num-workflows", Required = true,
        HelpText = "Number of workflows to execute")]
    public int NumWorkflows { get; set; }
    
    [Option('d', "depth", Required = true,
        HelpText = "Depth of each workflow to execute")]
    public int Depth { get; set; }
}

public class Program
{
    public static void Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());

        var random = new Random();
        var inputBytes = new byte[1 << 15];
        random.NextBytes(inputBytes);

        if (options.Type.Equals("client"))
        {
            using var channel = GrpcChannel.ForAddress("http://localhost:15721");
            var client = new WorkflowOrchestrator.WorkflowOrchestratorClient(channel);
            for (var i = 0; i < options.NumWorkflows; i++)
            {
                client.ExecuteWorkflow(new ExecuteWorkflowRequest
                {
                    WorkflowId = i,
                    Depth = options.Depth,
                    Input = ByteString.CopyFrom(inputBytes)
                });
                Console.WriteLine($"Workflow number {i} finished");
            }
            
        }
        else if (options.Type.Equals("orchestrator"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(new WorkflowOrchestratorServiceSettings
            {
                DarqSettings = null,
                Workers = null
            });
            var app = builder.Build();
            app.MapGrpcService<WorkflowOrchestratorService>();
            app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        }
        else if (options.Type.Equals("worker"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.Services.AddGrpc();
            var app = builder.Build();
            app.MapGrpcService<TaskExecutorService>();
            app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        }
        else
        {
            throw new NotImplementedException();
        }
    }
}