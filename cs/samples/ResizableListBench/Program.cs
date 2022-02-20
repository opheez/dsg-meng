using System;
using CommandLine;
using FASTER.core;

namespace epvs
{
    internal class Options
    {
        [Option('d', "data-structure-type", Required = true,
            HelpText = "data structure options:" +
                       "\n    latch-free-mock" +
                       "\n    latched" +
                       "\n    simple-version" +
                       "\n    two-phase-version")]
        public string DataStructureType { get; set; }

        [Option('o', "num-ops", Default = 1000000)]
        public int NumOps { get; set; }

        [Option('t', "num-threads", Required = true)]
        public int NumThreads { get; set; }
        
        [Option('n', "numa", Required = false, Default = 0,
            HelpText = "NUMA options:" +
                       "\n    0 = No sharding across NUMA sockets" +
                       "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('p', "push-probability", Default = 0.2)]
        public double PushProbability { get; set; }
        
        [Option('w', "write-probability", Default = 0.2)]
        public double WriteProbability { get; set; }
        
        [Option('r', "read-probability", Default = 0.6)]
        public double ReadProbability { get; set; }

        [Option('l', "dump-latency", Default = false)]
        public bool DumpLatencyMeasurements { get; set; }

        [Option('s', "latency-sample-rate", Default = 1e-4)]
        public double LatencySampleRate { get; set; }
        
        [Option('i', "initial-count", Default = 128)]
        public int InitialCount { get; set; }
        
        [Option('u', "output-file", Default = "")]
        public string OutputFile { get; set; }
    }


    internal class Program
    {
        static void Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<Options>(args).Value;
            if (Math.Abs(options.PushProbability + options.WriteProbability + options.ReadProbability - 1.0) > 1e-6)
                throw new FasterException("probability given does not add up to 1!");
            LightEpoch.InitializeStatic(128, 16);

            switch (options.DataStructureType)
            {
                case "latch-free-mock":
                    // Pretty unlikely for a randomly generated workload to present two times as many push operations
                    var loadFactor = Math.Min(1, 2 * options.PushProbability);
                    var estimatedSize = (int) Math.Ceiling(loadFactor * options.NumOps * options.NumThreads) + options.InitialCount;
                    new ResizableListBench<MockLatchFreeResizableList>(new MockLatchFreeResizableList(estimatedSize)).RunExperiment(options);
                    break;
                case "latched":
                    new ResizableListBench<LatchedResizableList>(new LatchedResizableList()).RunExperiment(options);
                    break;
                case "simple-version":
                    new ResizableListBench<SimpleVersionSchemeResizableList>(new SimpleVersionSchemeResizableList()).RunExperiment(options);

                    break;
                case "two-phase-version":
                    new ResizableListBench<TwoPhaseResizableList>(new TwoPhaseResizableList()).RunExperiment(options);
                    break;
                default:
                        throw new FasterException("Unrecognized data structure type");
            }
        }
    }
}