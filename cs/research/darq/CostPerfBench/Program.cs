using System.Diagnostics;
using CommandLine;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.devices;
using FASTER.libdpr;
using FASTER.server;

namespace microbench
{
    public class Options
    {
        [Option('n', "num-tasks", Required = false, Default = 1000,
            HelpText = "number of messages to fill DARQ with initially")]
        public int NumMessages { get; set; }

        [Option('c', "compute-scale", Required = false, Default = 1000000,
            HelpText = "each task computes pi in parallel to compute-scale iterations")]
        public int ComputeScale { get; set; }

        [Option('t', "darq-type", Required = true,
            HelpText = "type of DARQ backend to use, one of MEM, SSD, BLOB")]
        public string DarqType { get; set; }
        
        [Option('r', "remote-processor", Required = false, Default = false,
            HelpText = "whether to run a processor remotely")]
        public bool RemoteProcessor { get; set; }
        
        [Option('d', "colocate-darq", Required = false, Default = false,
            HelpText = "whether to use one DARQ underneath")]
        public bool ColocateDarq { get; set; }
    }

    public class Program
    {
        private static HardCodedClusterInfo clusterInfo;
        private const string CONN_STRING = "";

        private static DarqServer RunDarqServer(WorkerId me, Options options)
        {
            IDevice logDevice;
            switch (options.DarqType)
            {
                case "MEM":
                    logDevice = new LocalMemoryDevice((1L << 32), 1L << 30, 1);
                    break;
                case "SSD":
                    logDevice = new LocalStorageDevice($"D:\\w{me.guid}.log", deleteOnClose: true);
                    break;
                case "BLOB":
                    logDevice = new AzureStorageDevice(CONN_STRING, "test", "recovery", "log", deleteOnClose: true);
                    break;
                default:
                    throw new NotImplementedException();
            }

            var commitManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"D:\\log-commits{me.guid}"), false);
            // Clear in case of leftover files
            commitManager.RemoveAllCommits();

            var darqSettings = new DarqSettings
            {
                DprFinder = null,
                LogDevice = logDevice,
                PageSize = 1L << 24,
                MemorySize = 1L << 25,
                LogCommitManager = commitManager,
                LogCommitDir = $"D:\\log-commits{me.guid}",
                FastCommitMode = true,
                DeleteOnClose = false
            };

            var darqServer = new DarqServer(new DarqServerOptions
            {
                Address = "127.0.0.1",
                Port = 15721 + (int)me.guid,
                me = me,
                DarqSettings = darqSettings,
                ClusterInfo = clusterInfo,
                commitIntervalMilli = 1
            });
            return darqServer;
        }

        private static void RunDarqProcessor(DarqServer darqServer, Options options)
        {
            var me = darqServer.GetDarq().Me();
            var other = options.ColocateDarq ? me : new WorkerId((me.guid + 1) % 2);
            var processor = new BenchmarkProcessor(me, other, options.ComputeScale);

            IDarqProcessorClient client;
            if (options.RemoteProcessor)
                client =
                    new DarqProcessorClient("127.0.0.1", 15721 + (int)darqServer.GetDarq().Me().guid);
            else
                client = new ColocatedDarqProcessorClient(darqServer.GetDarq());
            client.StartProcessing(processor);
            darqServer.Dispose();
            // TODO(Tianyu): Need to explicitly deallocate underlying log?
        }


        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var options = result.MapResult(o => o, xs => new Options());

            // Compose cluster architecture
            clusterInfo = new HardCodedClusterInfo().SetDprFinder(null, 0)
                .AddWorker(new WorkerId(0), "", "127.0.0.1", 15721)
                .AddWorker(new WorkerId(1), "", "127.0.0.1", 15722);

            var darq1 = RunDarqServer(new WorkerId(0), options);
            var darq2 = RunDarqServer(new WorkerId(1), options);
            Thread t1, t2 = null;
            t1 = new Thread(() =>
            { 
                darq1.Start();
                RunDarqProcessor(darq1, options);
            });
            if (!options.ColocateDarq)
                t2 = new Thread(() =>
                {
                    darq2.Start();
                    RunDarqProcessor(darq2, options);
                });
            
            t1.Start();
            t2?.Start();

            // Give time for the new threads to setup
            Thread.Sleep(100);

            var darqClient = new DarqProducerClient(clusterInfo);
            // Send the initial message and then begin timing
            var stopwatch = Stopwatch.StartNew();
            darqClient.EnqueueMessageAsync(new WorkerId(0), BitConverter.GetBytes(options.NumMessages))
                .GetAwaiter().GetResult();
            t1.Join();
            t2?.Join();
            stopwatch.Stop();
            Console.WriteLine(1000.0 * options.NumMessages / stopwatch.ElapsedMilliseconds);
        }
    }
}