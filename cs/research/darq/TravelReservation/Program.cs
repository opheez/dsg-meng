// See https://aka.ms/new-console-template for more information

using System.Net;
using System.Text;
using CommandLine;
using Consul;
using darq;
using ExampleServices.spfaster;
using FASTER.core;
using FASTER.darq;
using FASTER.devices;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Net.Client;
using MathNet.Numerics.Distributions;
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
    public string Window { get; set; }
    
    [Option('n', "name", Required = false,
        HelpText = "name of the service to launch, if launching a worker")]
    public string WorkerName { get; set; }
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
            case "worker":
                await LaunchReservationService(options);
                break;
            case "dprfinder":
                await LaunchDprFinder(options);
                break;
            case "generate":
                await LaunchDprFinder(options);
                break;
            default:
                throw new NotImplementedException();
        }
    }
    

    private void GenerateWorkloadTrace(string fileName)
    {
        const int numClients = 1, numServices = 3, numWorkflowsPerSecond = 50, numSeconds = 60, numOfferings = 1000;
        var random = new Random();
        
        // Generate database
        // Over provision a to ensure we don't all abort
        const int numOfferingsRequired = (int)(numClients * numWorkflowsPerSecond * numSeconds * 1.2 / numOfferings);
        for (var i = 0; i < numServices; i++) 
        {
            using var writer = new StreamWriter($"{fileName}-service-{i}.csv");
            for (var j = 0; j < numOfferings; j++)
            {
                var builder = new StringBuilder();
                // offering id
                builder.Append(j);
                builder.Append(',');
                // entityId id
                builder.Append(random.NextInt64());
                builder.Append(',');
                // price
                builder.Append(random.Next(100, 300));
                builder.Append(',');
                // num reservable
                // TODO(Tianyu): Randomize a bit?
                builder.Append(numOfferingsRequired);
                writer.WriteLine(builder.ToString());
            }
        }
        
        // Generate workload
        var poisson = new Poisson(numWorkflowsPerSecond);
        for (var i = 0; i < numClients; i++)
        {
            List<long> requestTimestamps = new();
            for (var second = 0; second < numSeconds; second++)
            {
                var numRequests = poisson.Sample();
                for (var request = 0; request < numRequests; request++)
                    requestTimestamps.Add(1000* second + random.Next(1000));
            }
            requestTimestamps.Sort();
            var uniqueIds = new Dictionary<long, byte>();
            using var writer = new StreamWriter($"{fileName}-client-{i}.csv");
            foreach (var timestamp in requestTimestamps)
            {
                var builder = new StringBuilder();
                // Issue time
                builder.Append(timestamp);
                builder.Append(',');
                // Workflow Id -- must ensure uniqueness
                long id;
                do
                {
                    id = random.NextInt64() % numClients + i;
                } while (!uniqueIds.TryAdd(id, 0));
                builder.Append(id);
                for (var j = 0; j < numServices; j++)
                {                
                    builder.Append(',');
                    // Reservation Id -- must ensure uniqueness
                    do
                    {
                        id = random.NextInt64() % numClients + i;
                    } while (!uniqueIds.TryAdd(id, 0));
                    builder.Append(id);
                    builder.Append(',');
                    // offeringId
                    builder.Append(random.NextInt64(numOfferings));
                    builder.Append(',');
                    // customerId
                    builder.Append(random.NextInt64());
                    builder.Append(',');
                    // count 
                    // TODO(Tianyu): More than 1?
                    builder.Append(1);
                }
                writer.WriteLine(builder.ToString());
            }
        }
    }
    

    private static async Task LaunchBenchmarkClient(Options options)
    {

        // for (var i = 0; i < options.NumWorkflows; i++)
        // {
            // await semaphore.WaitAsync();
            // Get a channel per invocation so load-balancing can do its thing
            var channel = GrpcChannel.ForAddress("orchestrator:15721");
            // No speculation from external client perspective
            var client = new WorkflowOrchestrator.WorkflowOrchestratorClient(channel);
            Task.Run(() => client.ExecuteWorkflowAsync(new ExecuteWorkflowRequest
            {
                WorkflowId = 0,
                WorkflowClassId = 0,
                Input = null
            }));
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
        
        builder.Services.AddSingleton(new DarqSettings
        {
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("dprfinder:15721")),
            LogDevice = new ManagedLocalStorageDevice("./data.log", deleteOnClose:true),
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
        var client = new ConsulClient(configuration =>
        {
            // Set the address of the Consul agent
            configuration.Address = new Uri("http://localhost:8500");
        });
        
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15722,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        // TODO(Tianyu): configure
        builder.Services.AddSingleton(new FasterKVSettings<Key, Value>
        {
            IndexSize = 0,
            ConcurrencyControlMode = ConcurrencyControlMode.None,
            LogDevice = null,
            ObjectLogDevice = null,
            PageSize = 0,
            SegmentSize = 0,
            MemorySize = 0,
            MutableFraction = 0,
            ReadCopyOptions = default,
            PreallocateLog = false,
            KeySerializer = null,
            ValueSerializer = null,
            EqualityComparer = null,
            KeyLength = null,
            ValueLength = null,
            ReadCacheEnabled = false,
            ReadCachePageSize = 0,
            ReadCacheMemorySize = 0,
            ReadCacheSecondChanceFraction = 0,
            CheckpointManager = null,
            CheckpointDir = null,
            RemoveOutdatedCheckpoints = false,
            TryRecoverLatest = false,
            ThrottleCheckpointFlushDelayMs = 0,
            CheckpointVersionSwitchBarrier = false,
        });
        // TODO(Tianyu): Configure
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = default,
            DprFinder = null,
            CheckpointPeriodMilli = 0,
            RefreshPeriodMilli = 0
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