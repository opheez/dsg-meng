﻿using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using CommandLine;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using dse.services;

namespace TravelReservation;
public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('w', "workload-trace", Required = false,
        HelpText = "Workload trace file to use")]
    public string WorkloadTrace { get; set; }
    
    [Option('o', "output-file", Required = false,
        HelpText = "Name of file to output")]
    public string OutputFile { get; set; }

    [Option('n', "name", Required = false,
        HelpText = "identifier of the service to launch")]
    public int WorkerName { get; set; }
    
    [Option('s', "speculative", Required = false, Default = false,
        HelpText = "whether services proceed speculatively")]
    public bool Speculative { get; set; }
    
    [Option('i', "issue-window", Required = false, Default = 16,
        HelpText = "how many requests can be concurrently in-flight")]
    public int IssueWindow { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        // var environment = new LocalDebugEnvironment();
        var environment = new KubernetesLocalStorageEnvironment(true);
        ThreadPool.SetMinThreads(16, 16);
        switch (options.Type.Trim())
        {
            case "client":
                Console.WriteLine("Starting client");
                await LaunchBenchmarkClient(options, environment);
                break;
            case "orchestrator":
                Console.WriteLine("Starting orchestrator");
                await LaunchOrchestratorService(options, environment);
                break;
            case "service":
                Console.WriteLine("Starting reservation service");
                await LaunchReservationService(options, environment);
                break;
            case "dprfinder":
                Console.WriteLine("Starting DPR finder");
                await LaunchDprFinder(options, environment);
                break;
            case "generate":
                new WorkloadGenerator()
                    .SetNumClients(1)
                    .SetNumServices(10)
                    .SetNumWorkflowsPerSecond(10)
                    .SetNumSeconds(120)
                    .SetNumOfferings(10000)
                    .SetBaseFileName("C:\\Users\\tianyu\\Desktop\\workloads\\TravelReservation-latency\\workloads\\workload-10service")
                    .GenerateWorkloadTrace(new Random());
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private static async Task LaunchBenchmarkClient(Options options, IEnvironment environment)
    {
        Console.WriteLine("Parsing workload file...");
        var timedRequests = new List<(long, ExecuteWorkflowRequest)>();
        foreach (var line in File.ReadLines(options.WorkloadTrace))
        {
            var args = line.Split(',');
            var timestamp = long.Parse(args[0]);

            var request = new ExecuteWorkflowRequest
            {
                WorkflowId = long.Parse(args[1]),
                WorkflowClassId = 0,
                Input = ByteString.CopyFrom(line, Encoding.UTF8)
            };
            timedRequests.Add(ValueTuple.Create(timestamp, request));
        }

        Console.WriteLine("Creating gRPC connections...");
        // Keep a few channels around and reuse them 
        var channelPool = new List<GrpcChannel>();
        for (var i = 0; i < 8; i++)
            // k8 load-balancing will ensure that we get a spread of different orchestrators behind these channels
            channelPool.Add(GrpcChannel.ForAddress(environment.GetOrchestratorConnString()));
        var measurements = new ConcurrentBag<long>();
        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("Starting Workload...");
        var rateLimiter = new SemaphoreSlim(options.IssueWindow, options.IssueWindow);
        for (var i = 0; i < timedRequests.Count; i++)
        {
            var request = timedRequests[i];
            while (stopwatch.ElapsedMilliseconds <= request.Item1)
                await Task.Yield();
            var channel = channelPool[i % channelPool.Count];
            var client = new WorkflowOrchestrator.WorkflowOrchestratorClient(channel);
            await rateLimiter.WaitAsync();
            Task.Run(async () =>
            {
                Console.WriteLine($"Issuing request to start workflow id:{request.Item2.WorkflowId}, request content: {request.Item2.Input.ToString(Encoding.UTF8)}");
                await client.ExecuteWorkflowAsync(request.Item2);
                var endTime = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"workflow id:{request.Item2.WorkflowId} has completed in {endTime - request.Item1} milliseconds");
                measurements.Add(endTime - request.Item1);
                rateLimiter.Release();
            });
        }

        while (measurements.Count != timedRequests.Count)
            await Task.Delay(5);
        await WriteResults(options, environment, measurements);
    }

    private static async Task WriteResults(Options options, IEnvironment environment,ConcurrentBag<long> measurements)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in measurements)
            streamWriter.WriteLine(line);
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        await environment.PublishResultsAsync(options.OutputFile, memoryStream);
    }

    public static async Task LaunchOrchestratorService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetOrchestratorPort(options),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        
        var checkpointManager = environment.GetOrchestratorCheckpointManager(options);
        builder.Services.AddSingleton(new DarqSettings
        {
            MyDpr = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress(environment.GetDprFinderConnString())),
            LogDevice = environment.GetOrchestratorDevice(options),
            LogCommitManager = checkpointManager, 
            PageSize = 1L << 22,
            MemorySize = 1L << 30,
            SegmentSize = 1L << 30,
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5,
            FastCommitMode = true,
        });
        // TODO(Tianyu): Switch to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<Darq>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<Darq>());
        builder.Services.AddSingleton(new DarqMaintenanceBackgroundServiceSettings
        {
            morselSize = 512,
            batchSize = 16,
            // Workflow orchestrator DARQs never produce out messages
            producerFactory = null
        });

        var connectionPool = new ConcurrentDictionary<int, GrpcChannel>();
        var workflowFactories = new Dictionary<int, OrchestratorBackgroundProcessingService.WorkflowFactory>
            { { 0, (input, logger) => new ReservationWorkflowStateMachine(input, connectionPool, environment, options.Speculative, logger) } };
        builder.Services.AddSingleton(new OrchestartorBackgroundProcessingServiceSettings
        {
            workflowFactories = workflowFactories,
            speculative = options.Speculative
        });
        builder.Services.AddSingleton<OrchestratorBackgroundProcessingService>();
        builder.Services.AddSingleton<WorkflowOrchestratorService>();
        builder.Services.AddSingleton<DprServerInterceptor<WorkflowOrchestratorService>>();
        
        builder.Services.AddHostedService<OrchestratorBackgroundProcessingService>(provider =>
            provider.GetRequiredService<OrchestratorBackgroundProcessingService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        builder.Services.AddHostedService<DarqMaintenanceBackgroundService>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<WorkflowOrchestratorService>>(); });
        var app = builder.Build();
        
        app.MapGrpcService<WorkflowOrchestratorService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
        foreach (var channel in connectionPool.Values)
            channel.Dispose();
    }

    public static async Task LaunchDprFinder(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetDprFinderPort(),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        using var dprFinderServiceDevice = environment.GetDprFinderDevice();
        builder.Services.AddSingleton(dprFinderServiceDevice);
        builder.Services.AddSingleton<GraphDprFinderBackend>();
        builder.Services.AddSingleton<DprFinderGrpcBackgroundService>();
        builder.Services.AddSingleton<DprFinderGrpcService>();
        
        builder.Services.AddGrpc();
        builder.Services.AddHostedService<DprFinderGrpcBackgroundService>(provider =>
            provider.GetRequiredService<DprFinderGrpcBackgroundService>());
        var app = builder.Build();
        
        app.MapGrpcService<DprFinderGrpcService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }

    public static async Task LaunchReservationService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetServicePort(options),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        var checkpointManager = environment.GetServiceCheckpointManager(options);
        builder.Services.AddSingleton(new FasterKVSettings<Key, Value>
        {
            IndexSize = 1 << 24,
            LogDevice = environment.GetServiceDevice(options),
            PageSize = 1 << 25,
            SegmentSize = 1 << 30,
            MemorySize = 1 << 30,
            CheckpointManager = checkpointManager,
            TryRecoverLatest = false,
        });
        builder.Services.AddSingleton<FasterKV<Key, Value>>();
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress(environment.GetDprFinderConnString())),
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        });
        // TODO(Tianyu): Switch implementation to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<FasterKvReservationStateObject>();
        builder.Services.AddSingleton(new FasterKvReservationStartFile
        {
            file = options.WorkloadTrace
        });
        builder.Services.AddSingleton<FasterKvReservationBackgroundService>();
        
        builder.Services.AddSingleton<FasterKvReservationService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<FasterKvReservationStateObject>());
        builder.Services.AddSingleton<DprServerInterceptor<FasterKvReservationService>>();
        
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<FasterKvReservationService>>(); });
        builder.Services.AddHostedService<FasterKvReservationBackgroundService>(provider =>
            provider.GetRequiredService<FasterKvReservationBackgroundService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        var app = builder.Build();
        
        app.MapGrpcService<FasterKvReservationService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }
}