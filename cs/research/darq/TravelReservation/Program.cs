// See https://aka.ms/new-console-template for more information

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Azure.Storage.Blobs;
using CommandLine;
using darq;
using ExampleServices.spfaster;
using FASTER.core;
using FASTER.darq;
using FASTER.devices;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
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
    
    [Option('w', "workload-trace", Required = false,
        HelpText = "Workload trace file to use")]
    public string WorkloadTrace { get; set; }
    
    [Option('n', "name", Required = false,
        HelpText = "identifier of the service to launch")]
    public int WorkerName { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        switch (options.Type)
        {
            case "client":
                await LaunchBenchmarkClient(options);
                break;
            case "orchestrator":
                await LaunchOrchestratorService(options);
                break;
            case "service":
                await LaunchReservationService(options);
                break;
            case "dprfinder":
                await LaunchDprFinder(options);
                break;
            case "generate":
                new WorkloadGenerator()
                    .SetNumClients(1)
                    .SetNumServices(3)
                    .SetNumWorkflowsPerSecond(50)
                    .SetNumSeconds(60)
                    .SetNumOfferings(1000)
                    .SetBaseFileName(options.WorkloadTrace)
                    .GenerateWorkloadTrace(new Random());
                break;
            default:
                throw new NotImplementedException();
        }
    }


    private static async Task LaunchBenchmarkClient(Options options)
    {

        var timedRequests = new List<(long, ExecuteWorkflowRequest)>();
        unsafe
        {
            foreach (var line in File.ReadLines(options.WorkloadTrace))
            {
                var args = line.Split(',');
                Debug.Assert(args.Length == 6);
                var timestamp = long.Parse(args[0]);
                var buf = stackalloc long[4];
                buf[0] = long.Parse(args[2]);
                buf[1] = long.Parse(args[3]);
                buf[2] = long.Parse(args[4]);
                buf[3] = long.Parse(args[5]);

                var request = new ExecuteWorkflowRequest
                {
                    WorkflowId = long.Parse(args[1]),
                    WorkflowClassId = 0,
                    Input = ByteString.CopyFrom(new Span<byte>(buf, 32))
                };
                timedRequests.Add(ValueTuple.Create(timestamp, request));
            }
        }

        // Keep a few channels around and reuse them 
        var channelPool = new List<GrpcChannel>();
        for (var i = 0; i < 8; i++)
            // k8 load-balancing will ensure that we get a spread of different orchestrators behind these channels
            channelPool.Add(GrpcChannel.ForAddress("orchestrator:15721"));

        var measurements = new ConcurrentBag<long>();
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < timedRequests.Count; i++)
        {
            while (stopwatch.ElapsedMilliseconds <= timedRequests[i].Item1)
                await Task.Yield();
            var channel = channelPool[i % channelPool.Count];
            var client = new WorkflowOrchestrator.WorkflowOrchestratorClient(channel);
            Task.Run(async () =>
            {
                var startTime = stopwatch.ElapsedMilliseconds;
                await client.ExecuteWorkflowAsync(new ExecuteWorkflowRequest
                {
                    WorkflowId = 0,
                    WorkflowClassId = 0,
                    Input = null
                });
                var endTime = stopwatch.ElapsedMilliseconds;
                measurements.Add(endTime - startTime);
            });
        }

        while (measurements.Count != timedRequests.Count)
            await Task.Delay(5);
        await WriteResults(options, measurements);
    }

    private static async Task WriteResults(Options options, ConcurrentBag<long> measurements)
    {
        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");
        
        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient($"client{options.WorkerName}-result.txt");
        
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in measurements)
            streamWriter.WriteLine(line);
        await streamWriter.FlushAsync();
        
        memoryStream.Position = 0;
        await blobClient.UploadAsync(memoryStream, overwrite: true);
    }

    public static Task LaunchOrchestratorService(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        builder.Services.AddSingleton(new DarqBackgroundWorkerPoolSettings
        {
            numWorkers = 2
        });
        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        builder.Services.AddSingleton(new DarqSettings
        {
            MyDpr = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("dprfinder:15721")),
            LogDevice = new AzureStorageDevice(connString, "orchestrators", options.WorkerName.ToString(), "darq"),
            PageSize = 1L << 22,
            MemorySize = 1L << 28,
            SegmentSize = 1L << 30,
            CheckpointPeriodMilli = 5,
            RefreshPeriodMilli = 5,
            FastCommitMode = true,
        });
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<Darq>();
        builder.Services.AddSingleton<DarqBackgroundWorkerPool>();
        builder.Services.AddSingleton<WorkflowOrchestratorService>();
        
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<Darq>());
        builder.Services.AddSingleton<DprServerInterceptor<WorkflowOrchestratorService>>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<WorkflowOrchestratorService>>(); });

        var app = builder.Build();
        app.MapGrpcService<WorkflowOrchestratorService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        return app.RunAsync();
    }

    public static Task LaunchDprFinder(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15720,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        using var device1 = new AzureStorageDevice(connString, "dprfinder", "recoverlog", "1", deleteOnClose:true);
        using var device2 = new AzureStorageDevice(connString, "dprfinder", "recoverlog", "2", deleteOnClose:true);
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
        return app.RunAsync();
    }

    public static Task LaunchReservationService(Options options)
    {
        
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15722,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        using var faster = new FasterKV<Key, Value>(1 << 20, new LogSettings
        {
            LogDevice = new AzureStorageDevice(connString, "services", options.WorkerName.ToString(), "log"),
            PageSizeBits = 25,
            SegmentSizeBits = 30,
            MemorySizeBits = 28
        }, new CheckpointSettings
        {
            CheckpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(connString),
                new DefaultCheckpointNamingScheme($"services/{options.WorkerName}/checkpoints/"))
        });
        builder.Services.AddSingleton(faster);
        
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("dprfinder:15721")),
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        });
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<FasterKvReservationStateObject>();
        builder.Services.AddSingleton<FasterKvReservationService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<FasterKvReservationStateObject>());
        builder.Services.AddSingleton<DprServerInterceptor<FasterKvReservationService>>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<FasterKvReservationService>>(); });
        var app = builder.Build();
        app.MapGrpcService<FasterKvReservationService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        return app.RunAsync();
    }
}