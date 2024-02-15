// See https://aka.ms/new-console-template for more information

using System.Net;
using CommandLine;
using darq;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core.Interceptors;
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
            var client =
            new WorkflowOrchestrator.WorkflowOrchestratorClient(
            channel.Intercept(new DprClientInterceptor(new DprSession())));
            for (var i = 0; i < options.NumWorkflows; i++)
            {
                client.ExecuteWorkflowAsync(new ExecuteWorkflowRequest
                {
                    WorkflowId = i,
                    Depth = options.Depth,
                    Input = ByteString.CopyFrom(inputBytes)
                }).GetAwaiter().GetResult();
                Console.WriteLine($"Workflow number {i} finished");
            }
        }
        else if (options.Type.Equals("orchestrator"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                serverOptions.Listen(IPAddress.Loopback, 15721,
                    listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            });

            using var dprFinderChannel = GrpcChannel.ForAddress("http://localhost:15720");
            var darqSettings = new DarqSettings
            {
                DprFinder = new GrpcDprFinder(dprFinderChannel),
                LogDevice = new LocalStorageDevice($"D:\\w0\\data.log", deleteOnClose: true),
                PageSize = 1L << 22,
                MemorySize = 1L << 28,
                SegmentSize = 1L << 30,
                CheckpointPeriodMilli = 10,
                RefreshPeriodMilli = 5,
                FastCommitMode = true,
                DeleteOnClose = true,
                CleanStart = true
            };
            using var workerChannel = GrpcChannel.ForAddress("http://localhost:15722");
            var executors = new List<GrpcChannel> { workerChannel };

            using var workerPool = new DarqBackgroundWorkerPool(2);
            var orchestratorService = new WorkflowOrchestratorService(new WorkflowOrchestratorServiceSettings
            {
                DarqSettings = darqSettings,
                Executors = executors,
                WorkerPool = workerPool
            });
            builder.Services.AddSingleton<DprWorker<DarqStateObject, RwLatchVersionScheme>>(orchestratorService
                .GetBackend());
            builder.Services.AddSingleton<DprServerInterceptor<DarqStateObject>>();
            builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<DarqStateObject>>(); });
            // TODO(Tianyu): Probably want to do some more DI shit and leave a per-request service handler and
            // configure background services, but who cares
            builder.Services.AddSingleton(orchestratorService);

            var app = builder.Build();
            app.MapGrpcService<WorkflowOrchestratorService>();
            app.MapGet("/",
                () =>
                    "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
            app.Run();
        }
        else if (options.Type.Equals("worker"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                serverOptions.Listen(IPAddress.Loopback, 15722,
                    listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            });
            builder.Services.AddSingleton<DprStatelessServerInterceptor>();
            builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprStatelessServerInterceptor>(); });
            var app = builder.Build();
            app.MapGrpcService<TaskExecutorService>();
            app.MapGet("/",
                () =>
                    "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
            app.Run();
        }
        else if (options.Type.Equals("dprfinder"))
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.ConfigureKestrel(serverOptions =>
            {
                serverOptions.Listen(IPAddress.Loopback, 15720,
                    listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            });
            var dprFinderServiceDevice = new PingPongDevice(new LocalMemoryDevice(1L << 30, 1L << 30, 1),
                new LocalMemoryDevice(1L << 30, 1L << 30, 1));
            var backend = new GraphDprFinderBackend(dprFinderServiceDevice);
            builder.Services.AddSingleton(new DprFinderGrpcService(backend));
            builder.Services.AddGrpc();
            var app = builder.Build();
            app.MapGrpcService<DprFinderGrpcService>();
            app.MapGet("/",
                () =>
                    "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
            app.Run();
        }
        else
        {
            throw new NotImplementedException();
        }
    }
}