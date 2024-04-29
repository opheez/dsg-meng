
using System.Diagnostics;
using System.Net;
using Azure.Storage.Blobs;
using CommandLine;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.proto;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace microbench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }
    
    [Option('o', "output-file", Required = false,
        HelpText = "Name of file to output")]
    public string OutputFile { get; set; }

    [Option('n', "num-workers", Required = false, Default = 1,
        HelpText = "number of workers to simulate in total in a pod")]
    public int NumWorkers { get; set; }
    
    [Option('i', "pod-id", Required = false,
        HelpText = "id of the client being launched")]
    public int PodId { get; set; }
    
    [Option('p', "num-pods", Required = false, Default = 8,
        HelpText = "number of pods")]
    public int NumPods { get; set; }
    
    [Option('d', "dep-prob", Required = false, Default = 0.2,
        HelpText = "probability of taking on a dependency")]
    public double DependencyProbability { get; set; }
    
    [Option('c', "checkpoint-interval", Required = false, Default = 10,
        HelpText = "checkpoint interval")]
    public int CheckpointInterval { get; set; }
}

public class StatsAggregationServiceImpl : StatsAggregationService.StatsAggregationServiceBase
{
    private int numPods;
    private TaskCompletionSource tcs = new();
    private List<long> measurements = new();
    private Action<List<long>> outputAction;
    
    public StatsAggregationServiceImpl(int numPods, Action<List<long>> outputAction)
    {
        this.numPods = numPods;
        this.outputAction = outputAction;
    }
    
    public override async Task<ReportResultsMessage> ReportResults(ReportResultsMessage request, ServerCallContext context)
    {
        lock (this)
        {
            measurements.AddRange(request.Latencies);
            if (--numPods == 0)
            {
                outputAction(measurements);
                tcs.SetResult();
            }
        }

        await tcs.Task;
        return new ReportResultsMessage();
    }
}


public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());

        switch (options.Type.Trim())
        {
            case "worker":
                await LaunchBenchmarkClient(options);
                break;
            case "dprfinder":
                await LaunchDprFinder(options);
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private static Task LaunchBenchmarkClient(Options options)
    {
        var threads = new List<Thread>();
        var workers = new List<DprWorkerId>();
        for (var i = 0; i < options.NumPods * options.NumWorkers; i++)
            workers.Add(new DprWorkerId(i));
        
        for (var i = 0; i < options.NumWorkers; i++)
        {
            var i1 = i * options.NumPods + options.PodId;
            threads.Add(new Thread(() =>
            {
                var channel = GrpcChannel.ForAddress("http://dprfinder.dse.svc.cluster.local:15721");
                var finder = new GrpcDprFinder(channel);
                var worker = new SimulatedDprWorker(finder, new UniformWorkloadGenerator(options.DependencyProbability), workers, new DprWorkerId(i1));
                worker.RunContinuously(30, options.CheckpointInterval);
                var client = new StatsAggregationService.StatsAggregationServiceClient(channel);
                var results = new ReportResultsMessage();
                results.Latencies.AddRange(worker.ComputeVersionCommitLatencies());
                client.ReportResults(results);
            }));
        }
        foreach (var thread in threads)
            thread.Start();
        
        foreach (var thread in threads)
            thread.Join();
        
        return Task.CompletedTask;
    }
    
    public static async Task LaunchDprFinder(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });

        using var device1 = new LocalMemoryDevice(1 << 25, 1 << 25, 1);
        using var device2 = new LocalMemoryDevice(1 << 25, 1 << 25, 1);
        var device = new PingPongDevice(device1, device2);

        builder.Services.AddSingleton(device);
        builder.Services.AddSingleton<GraphDprFinderBackend>();
        builder.Services.AddSingleton<DprFinderGrpcBackgroundService>();
        builder.Services.AddSingleton<DprFinderGrpcService>();
        var aggregation = new StatsAggregationServiceImpl(options.NumWorkers,  measurements =>
        {
            // foreach (var line in measurements)
                // Console.WriteLine(line * 1000.0 / Stopwatch.Frequency);
            using var memoryStream = new MemoryStream(); 
            using var streamWriter = new StreamWriter(memoryStream);
            foreach (var line in measurements)
                streamWriter.WriteLine(line);
            streamWriter.Flush();
            memoryStream.Position = 0;
            
            var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
            var blobServiceClient = new BlobServiceClient(connString);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");
            
            blobContainerClient.CreateIfNotExists();
            var blobClient = blobContainerClient.GetBlobClient(options.OutputFile);
            blobClient.Upload(memoryStream, overwrite: true);
        });
        
        builder.Services.AddSingleton(aggregation);

        builder.Services.AddGrpc();
        builder.Services.AddHostedService<DprFinderGrpcBackgroundService>(provider =>
            provider.GetRequiredService<DprFinderGrpcBackgroundService>());
        var app = builder.Build();

        app.MapGrpcService<DprFinderGrpcService>();
        app.MapGrpcService<StatsAggregationServiceImpl>();

        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }
}