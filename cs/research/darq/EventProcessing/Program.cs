// See https://aka.ms/new-console-template for more information
using System.Net;
using Azure.Storage.Blobs;
using CommandLine;
using Consul;
using FASTER.core;
using FASTER.darq;
using FASTER.devices;
using FASTER.libdpr;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using dse.services;

namespace EventProcessing;

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
        switch (options.Type.Trim())
        {
            case "client":
                await LaunchBenchmarkClient(options);
                break;
            case "filter":
            case "aggregate":
            case "detection":
                await LaunchProcessor(options);
                break;
            case "worker":
                await LaunchPubsubService(options);
                break;
            case "dprfinder":
                await LaunchDprFinder(options);
                break;
            case "generate":
                new SearchListDataGenerator().SetOutputFile(options.WorkloadTrace)
                    .SetSearchTermRelevantProb(0.2)
                    .SetTrendParameters(0.1, 250000, 100000)
                    .SetSearchTermLength(80)
                    .SetThroughput(250000)
                    .SetNumSearchTerms(250000 * 30)
                    .Generate();
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private static async Task LaunchBenchmarkClient(Options options)
    {
        var client = new SpPubSubServiceClient(new ConsulClient(new ConsulClientConfiguration
        {
            Address = new Uri("consul:8500"),
        }));
        var loader = new SearchListDataLoader(options.WorkloadTrace, client, 0);
        // TODO(Tianyu): hardcoded numbers for two host machines
        await client.CreateTopic(3, "1", "host1:15721", new DprWorkerId(3));
        loader.LoadData();
        Task.Run(loader.Run);
        var processingClient = new SpPubSubProcessorClient(3, client);
        var measurementProcessor = new SearchListLatencyMeasurementProcessor();
        Task.Run(async () => await processingClient.StartProcessingAsync(measurementProcessor));
        await measurementProcessor.workloadTerminationed.Task;
        await WriteResults(options, measurementProcessor);
    }

    private static async Task WriteResults(Options options, SearchListLatencyMeasurementProcessor processor)
    {
        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");

        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient($"events-result.txt");

        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in processor.results)
            streamWriter.WriteLine($"{line.Key}, {line.Value.Item1}, {line.Value.Item2}");
        await streamWriter.FlushAsync();

        memoryStream.Position = 0;
        await blobClient.UploadAsync(memoryStream, overwrite: true);
    }

    public static Task LaunchPubsubService(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });

        var connString = Environment.GetEnvironmentVariable("AZURE_CONN_STRING");
        builder.Services.AddSingleton(new SpPubSubServiceSettings
        {
            consulConfig = new ConsulClientConfiguration
            {
                Address = new Uri("consul:8500"),
            },
            factory = (id, dprId) => new Darq(new DarqSettings
            {
                Me = new DarqId(id),
                MyDpr = dprId,
                DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("dprfinder:15721")),
                LogDevice = new AzureStorageDevice(connString, "DARQs", options.WorkerName.ToString(), $"darq{id}"),
                PageSize = 1L << 22,
                MemorySize = 1L << 28,
                SegmentSize = 1L << 30,
                CheckpointPeriodMilli = 5,
                RefreshPeriodMilli = 5,
                FastCommitMode = true
            }, new RwLatchVersionScheme()),
            hostId = options.WorkerName.ToString(),
        });

        builder.Services.AddSingleton<SpPubSubService>();
        var app = builder.Build();
        app.MapGrpcService<SpPubSubService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        return app.RunAsync();
    }

    public static async Task LaunchProcessor(Options options)
    {
        var client = new SpPubSubServiceClient(new ConsulClient(new ConsulClientConfiguration
        {
            Address = new Uri("consul:8500"),
        }));
        // TODO(Tianyu): hardcoded numbers for two host machines
        var outputTopic = options.Type switch
        {
            "filter" => 1,
            "aggregate" => 2,
            "detection" => 3,
            _ => throw new ArgumentOutOfRangeException()
        };
        var hostId = outputTopic % 2;
        await client.CreateTopic(outputTopic, hostId.ToString(),
            $"host{hostId}:15721",
            new DprWorkerId(outputTopic));
        var processingClient = new SpPubSubProcessorClient(outputTopic - 1, client);
        SpPubSubEventHandler handler = options.Type switch
        {
            "filter" => new FilterAndMapEventProcessor(outputTopic),
            "aggregate" => new AggregateEventProcessor(outputTopic),
            "detection" => new AnomalyDetectionEventProcessor(outputTopic, 0.1),
            _ => throw new ArgumentOutOfRangeException()
        };
        await processingClient.StartProcessingAsync(handler);
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
        using var device1 = new AzureStorageDevice(connString, "dprfinder", "recoverlog", "1", deleteOnClose: true);
        using var device2 = new AzureStorageDevice(connString, "dprfinder", "recoverlog", "2", deleteOnClose: true);
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
}