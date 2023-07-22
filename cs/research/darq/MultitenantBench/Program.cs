using System.Diagnostics;
using CommandLine;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;

namespace microbench
{
    public class Options
    {
        [Option('n', "num-tasks", Required = false, Default = 1000000,
            HelpText = "number of messages to fill DARQ with initially")]
        public int NumMessages { get; set; }

        [Option('c', "compute-scale", Required = false, Default = 1000,
            HelpText = "each task computes pi in parallel to compute-scale * 1000_000 iterations")]
        public int ComputeScale { get; set; }

        [Option('t', "num-tenants", Required = true,
            HelpText = "number of tenants to put on a single DARQ")]
        public int NumTenants { get; set; }
        
        [Option('i', "checkpoint-interval", Required = false, Default = 5,
            HelpText = "checkpoint interval of DARQ, in milli")]
        public int CheckpointInterval { get; set; }
    }

    public unsafe class Program
    {
        private static HardCodedClusterInfo clusterInfo;

        public static void RunDarq(Options options)
        {
                        
            var logDevice = new LocalStorageDevice($"E:\\w0.log", deleteOnClose: true);
            var commitManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"E:\\log-commits"), false);
            // Clear in case of leftover files
            commitManager.RemoveAllCommits();

            var darqSettings = new DarqSettings
            {
                DprFinder = null,
                LogDevice = logDevice,
                PageSize = 1L << 24,
                MemorySize = 1L << 25,
                LogCommitManager = commitManager,
                LogCommitDir = $"E:\\log-commits",
                FastCommitMode = true,
                DeleteOnClose = false
            };

            var darqServer = new DarqServer(new DarqServerOptions
            {
                Address = "127.0.0.1",
                Port = 15721,
                me = new WorkerId(0),
                DarqSettings = darqSettings,
                ClusterInfo = clusterInfo,
                commitIntervalMilli = options.CheckpointInterval
            });
            darqServer.Start();
            var processor = new BenchmarkProcessor(new WorkerId(0), options.NumTenants, options.ComputeScale);
            var client =
                new ColocatedDarqProcessorClient(darqServer.GetDarq());
            Console.WriteLine("Starting processor...");
            client.StartProcessing(processor);
            darqServer.Dispose();
        }

        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var options = result.MapResult(o => o, xs => new Options());
            
            // Compose cluster architecture
            clusterInfo = new HardCodedClusterInfo().SetDprFinder(null, 0)
                .AddWorker(new WorkerId(0), "", "127.0.0.1", 15721);

            var thread = new Thread(() =>
            {
                RunDarq(options);
            });
            thread.Start();

            // Give time for the new threads to setup
            Thread.Sleep(100);

            var darqClient = new DarqProducerClient(clusterInfo);
            // Send the initial message and then begin timing
            var stopwatch = Stopwatch.StartNew();
            for (var i = 0; i < options.NumTenants; i++)
            {
                Span<byte> buf = stackalloc byte[8];
                BitConverter.TryWriteBytes(buf, i);
                BitConverter.TryWriteBytes(buf.Slice(sizeof(int)), options.NumMessages);
                darqClient.EnqueueMessageAsync(new WorkerId(0), buf, forceFlush: false);
            }
            darqClient.ForceFlush();
            thread.Join();
            stopwatch.Stop();
            Console.WriteLine(1000.0 * options.NumMessages * options.NumTenants / stopwatch.ElapsedMilliseconds);
        }
    }
}