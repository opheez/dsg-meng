using System.Net;
using CommandLine;
using dse.services;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using protobuf;
using Task = System.Threading.Tasks.Task;

namespace microbench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        if (options.Type.Equals("dprfinder"))
            await LaunchDprFinder();
        else if (options.Type.Equals("faster"))
        {
            for (var i = 0; i < 3; i++)
            {
                var i1 = i;
                Task.Run(() => LaunchReservationService(15721 + i1));
            }

            await Task.Delay(Timeout.InfiniteTimeSpan);
        }
        else
        {
            var requests0 = new List<ReservationRequest>();
            var requests1 = new List<ReservationRequest>();
            var requests2 = new List<ReservationRequest>();
            foreach (var line in File.ReadLines(
                         "C:\\Users\\tianyu\\Documents\\FASTER\\cs\\research\\darq\\workloads\\workload-micro-faster-client.csv"))
            {
                var split = line.Split(',');
                requests0.Add(new ReservationRequest
                {
                    ReservationId = long.Parse(split[2]),
                    OfferingId = long.Parse(split[3]),
                    CustomerId = long.Parse(split[4]),
                    Count = int.Parse(split[5])
                });
                requests1.Add(new ReservationRequest
                {
                    ReservationId = long.Parse(split[6]),
                    OfferingId = long.Parse(split[7]),
                    CustomerId = long.Parse(split[8]),
                    Count = int.Parse(split[9])
                });
                requests2.Add(new ReservationRequest
                {
                    ReservationId = long.Parse(split[10]),
                    OfferingId = long.Parse(split[11]),
                    CustomerId = long.Parse(split[12]),
                    Count = int.Parse(split[13])
                });
            }

            // var numTasks = 8;
            // var ev = new CountdownEvent(numTasks);
            // for (var i = 0; i < numTasks; i++)
            // {
            //     var i1 = i;
            //     Task.Run(async () =>
            //     {
            //         var channel = GrpcChannel.ForAddress("http://127.0.0.1:15721");
            //         var session = new DprSession();
            //         var client = new FasterKVReservationService.FasterKVReservationServiceClient(
            //             channel.Intercept(new DprClientInterceptor(session)));
            //         for (var j = i1; j < requests.Count; j += numTasks)
            //             await client.MakeReservationAsync(requests[j]);
            //         var dprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("http://127.0.0.1:15722"));
            //         await session.SpeculationBarrier(dprFinder, true);
            //         ev.Signal();
            //     });
            // }
            // ev.Wait();

            // var channel0 = GrpcChannel.ForAddress("http://127.0.0.1:15721");
            // var channel1 = GrpcChannel.ForAddress("http://127.0.0.1:15722");
            // var channel2 = GrpcChannel.ForAddress("http://127.0.0.1:15723");
            //
            // var client0 = new FasterKVReservationService.FasterKVReservationServiceClient(channel0);
            // var client1 = new FasterKVReservationService.FasterKVReservationServiceClient(channel0);
            // var client2 = new FasterKVReservationService.FasterKVReservationServiceClient(channel0);
            //
            // var semaphore = new SemaphoreSlim(8, 8);
            // for (var i = 0; i < requests0.Count; i++)
            // {
            //     await semaphore.WaitAsync();
            //     var i1 = i;
            //     Task.Run(async () =>
            //     {
            //         await client.MakeReservationAsync(requests[i1]);
            //         semaphore.Release();
            //     });
            // }
            //
            // await semaphore.WaitAsync();
        }
    }

    public static async Task LaunchDprFinder()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15722,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        using var device1 = new LocalMemoryDevice(1 << 28, 1 << 28, 1);
        using var device2 = new LocalMemoryDevice(1 << 28, 1 << 28, 1);
        var finderDevice = new PingPongDevice(device1, device2);
        builder.Services.AddSingleton(finderDevice);
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

    public static async Task LaunchReservationService(int port)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, port,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        var checkpointManager = new DeviceLogCommitCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme("D:\\reservation\\checkpoints"), removeOutdated: false);
        checkpointManager.PurgeAll();

        builder.Services.AddSingleton(new FasterKVSettings<Key, Value>
        {
            IndexSize = 1 << 24,
            LogDevice = new LocalStorageDevice($"D:\\reservation.log", deleteOnClose: true),
            PageSize = 1 << 25,
            SegmentSize = 1 << 28,
            MemorySize = 1 << 28,
            CheckpointManager = checkpointManager,
            TryRecoverLatest = false,
        });
        builder.Services.AddSingleton<FasterKV<Key, Value>>();
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(0),
            DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress("http://127.0.0.1:15722")),
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        });
        // TODO(Tianyu): Switch implementation to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<FasterKvReservationStateObject>();
        builder.Services.AddSingleton(new FasterKvReservationStartFile
        {
            file = "C:\\Users\\tianyu\\Documents\\FASTER\\cs\\research\\darq\\workloads\\workload-micro-faster.csv"
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