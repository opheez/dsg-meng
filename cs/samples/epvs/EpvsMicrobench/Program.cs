using System.Security.Cryptography;
using System.Threading;
using CommandLine;
using FASTER.core;

namespace EpvsMicrobench
{
    internal class Options
    {
        [Option('o', "num-ops", Default = 10000000)]
        public int NumOps { get; set; }

        [Option('t', "table-size", Default = 128)]
        public int EpochTableSize { get; set; }

        [Option('d', "drainlist-size", Default = 16)]
        public int DrainListSize { get; set; }

        [Option('n', "num-threads", Required = true)]
        public int NumThreads { get; set; }

        [Option('p', "probability", Default = 0.001)]
        public int VersionChangeProbability { get; set; }
        
        [Option('l', "delay", Default = 1)]
        public int VersionChangeDelay { get; set; }
    }


    internal class Program
    {
        private static byte[] hashBytes;
        private static ThreadLocal<HashAlgorithm> hasher;


        static void DoWork(int numUnits, byte[] dest)
        {
            for (var i = 0; i < numUnits; i++)
                hasher.Value.TryComputeHash(hashBytes, dest, out _);
        }


        static void ExecuteBenchmarkThread(Options options, EpochProtectedVersionScheme tested)
        {
            
        }


        static void Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<Options>(args).Value;
            
        }
    }
}