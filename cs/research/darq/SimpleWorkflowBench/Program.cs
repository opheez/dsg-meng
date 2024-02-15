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
        switch (options.Type)
        {
            case "client":
                LaunchBenchmarkClient(options);
                break;
            case "orchestrator":
                LaunchOrchestratorService(options);
                break;
            case "worker":
                LaunchExecutor(options);
                break;
            case "dprfinder":
                LaunchDprFinder(options);
                break;
            default:
                throw new NotImplementedException();
        }
    }

    public static void LaunchBenchmarkClient(Options options)
    {
        using var channel = GrpcChannel.ForAddress("http://localhost:15721");
        var client =
            new WorkflowOrchestrator.WorkflowOrchestratorClient(
                channel.Intercept(new DprClientInterceptor(new DprSession())));
        var random = new Random();
        var inputBytes = new byte[1 << 15];
        for (var i = 0; i < options.NumWorkflows; i++)
        {
            random.NextBytes(inputBytes);
            client.ExecuteWorkflowAsync(new ExecuteWorkflowRequest
            {
                WorkflowId = i,
                Depth = options.Depth,
                Input = ByteString.CopyFrom(inputBytes)
            }).GetAwaiter().GetResult();
            Console.WriteLine($"Workflow number {i} finished");
        }
    }

    public static void LaunchOrchestratorService(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Loopback, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        
        using var workerChannel = GrpcChannel.ForAddress("http://localhost:15722");
        var executors = new List<GrpcChannel> { workerChannel };
        builder.Services.AddSingleton(executors);
        builder.Services.AddSingleton(new DarqBackgroundWorkerPoolSettings
        {
            numWorkers = 2
        });
        
        using var dprFinderChannel = GrpcChannel.ForAddress("http://localhost:15720");
        builder.Services.AddSingleton(new DarqSettings
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
        });
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<Darq>();
        builder.Services.AddSingleton<DprWorker<DarqStateObject>>(sp => sp.GetService<Darq>());
        builder.Services.AddSingleton<DarqBackgroundWorkerPool>();
        builder.Services.AddSingleton<WorkflowOrchestratorService>();
        builder.Services.AddSingleton<DprServerInterceptor<DarqStateObject, WorkflowOrchestratorService>>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<DarqStateObject, WorkflowOrchestratorService>>(); });

        var app = builder.Build();
        app.MapGrpcService<WorkflowOrchestratorService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        app.Run();
    }

    public static void LaunchDprFinder(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Loopback, 15720,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        using var device1 = new LocalMemoryDevice(1L << 30, 1L << 30, 1);
        using var device2 = new LocalMemoryDevice(1L << 30, 1L << 30, 1);
        var dprFinderServiceDevice = new PingPongDevice(device1, device2);
        builder.Services.AddSingleton(dprFinderServiceDevice);
        builder.Services.AddSingleton<GraphDprFinderBackend>();
        builder.Services.AddSingleton<DprFinderGrpcService>();
        builder.Services.AddGrpc();
        var app = builder.Build();
        app.MapGrpcService<DprFinderGrpcService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        app.Run();
    }

    private static void LaunchExecutor(Options options)
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
}