using System.Security.Cryptography;
using System.Threading;
using CommandLine;
using FASTER.core;

namespace epvs
{
    internal class Options
    {
        [Option('o', "num-ops", Default = 10000000)]
        public int NumOps { get; set; }

        [Option('e', "table-size", Default = 128)]
        public int EpochTableSize { get; set; }

        [Option('d', "drainlist-size", Default = 16)]
        public int DrainListSize { get; set; }

        [Option('t', "num-threads", Required = true)]
        public int NumThreads { get; set; }
        
        [Option('n', "numa", Required = false, Default = 0,
            HelpText = "NUMA options:" +
                       "\n    0 = No sharding across NUMA sockets" +
                       "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('p', "probability", Default = 0.001)]
        public int VersionChangeProbability { get; set; }
        
        [Option('l', "delay", Default = 5)]
        public int VersionChangeDelay { get; set; }
    }


    internal class Program
    {
        static void Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<Options>(args).Value;
            var bench = new EpvsBench();
            bench.RunExperiment(options);
        }
    }
}