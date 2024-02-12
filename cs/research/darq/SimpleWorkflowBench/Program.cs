// See https://aka.ms/new-console-template for more information

using System.Net;
using CommandLine;
using FASTER.core;
using FASTER.darq;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using SimpleWorkflowBench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('n', "num-workflows", Required = false,
        HelpText = "Number of workflows to execute")]
    public int NumWorkflows { get; set; }
    
    [Option('d', "depth", Required = false,
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
            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                serverOptions.Listen(IPAddress.Loopback, 15721, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });
            builder.Services.AddGrpc();
            
            var darqSetting = new DarqSettings
            {
                LogDevice =  new LocalStorageDevice($"D:\\w0\\data.log", deleteOnClose: true),
                PageSize = 1L << 22,
                MemorySize = 1L << 28,
                SegmentSize = 1L << 30,
                FastCommitMode = true,
                DeleteOnClose = true,
                CleanStart = true
            };
            var workers = new List<TaskExecutor.TaskExecutorClient>();
            var channel = GrpcChannel.ForAddress("http://localhost:15722");           
            workers.Add(new TaskExecutor.TaskExecutorClient(channel));
            
            builder.Services.AddSingleton(new WorkflowOrchestratorServiceSettings
            {
                DarqSettings = darqSetting,
                Workers = workers
            });
            // TODO(Tianyu): Probably want to do some more DI shit and leave a per-request service handler, but who cares
            builder.Services.AddSingleton<WorkflowOrchestratorService>();
            var app = builder.Build();
            app.Lifetime.ApplicationStopping.Register(() => channel.Dispose());
            
            app.MapGrpcService<WorkflowOrchestratorService>();
            app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
            app.Run();
        }
        else if (options.Type.Equals("worker"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                serverOptions.Listen(IPAddress.Loopback, 15722, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });
            builder.Services.AddGrpc();
            var app = builder.Build();
            app.MapGrpcService<TaskExecutorService>();
            app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
            app.Run();
        }
        else
        {
            throw new NotImplementedException();
        }
    }
}