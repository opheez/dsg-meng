using CommandLine;

namespace epvs
{
    internal class Options
    {
        [Option('m', "synchronization-mode", Default = "epvs",
        HelpText = "synchronization mode options:" +
        "\n    latch-free" +
        "\n    epvs" +
        "\n    latch")]
        public string SynchronizationMode { get; set; }

        [Option('o', "num-ops", Default = 1000000)]
        public int NumOps { get; set; }

        [Option('e', "table-size", Default = 512)]
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

        [Option('p', "probability", Default = 1e-6)]
        public double VersionChangeProbability { get; set; }
        
        [Option('l', "delay", Default = 1)]
        public int VersionChangeDelay { get; set; }
        
                
        [Option('u', "output-file", Default = "")]
        public string OutputFile { get; set; }
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