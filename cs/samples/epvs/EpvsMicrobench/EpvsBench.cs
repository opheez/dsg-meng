using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.core;

namespace epvs
{
    internal class EpvsBench
    {
        internal SimpleVersionScheme tested;
        internal byte[] hashBytes;

        internal class Worker
        {
            // private byte[] scratchPad;
            // private HashAlgorithm hasher;
            private long scratchPad;
            private EpvsBench parent;
            private List<int> versionChangeIndexes;
            private int numOps, versionChangeDelay, numaStyle, threadId;

            internal Worker(EpvsBench parent, Options options, Random random, int threadId)
            {
                // hasher = new SHA256Managed();
                // scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                versionChangeIndexes = new List<int>();
                numOps = options.NumOps;
                versionChangeDelay = options.VersionChangeDelay;
                numaStyle = options.NumaStyle;
                this.threadId = threadId;

                for (var i = 0; i < numOps; i++)
                {
                    if (random.NextDouble() < options.VersionChangeProbability)
                        versionChangeIndexes.Add(i);
                }
            }

            private void DoWork(int numUnits)
            {
                for (var i = 0; i < numUnits; i++)
                    // hasher.TryComputeHash(parent.hashBytes, scratchPad, out _);
                    scratchPad++;
            }
            

            internal void RunOneThread()
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint) threadId);
                else
                    Native32.AffinitizeThreadShardedNuma((uint) threadId, 2); // assuming two NUMA sockets

                var nextChangeIndex = 0;
                for (var i = 0; i < numOps; i++)
                {
                    if (nextChangeIndex < versionChangeIndexes.Count && i == versionChangeIndexes[nextChangeIndex])
                    {
                        parent.tested.AdvanceVersion((_, _) => DoWork(versionChangeDelay));
                        nextChangeIndex++;
                    }
                    else
                    {
                        parent.tested.Enter();
                        DoWork(1);
                        parent.tested.Leave();
                    }
                }
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[16];
            new Random().NextBytes(hashBytes);
            LightEpoch.InitializeStatic(options.EpochTableSize, options.DrainListSize);
            tested = new SimpleVersionScheme();

            var threads = new List<Thread>();
            var random = new Random();
            for (var i = 0; i < options.NumThreads; i++)
            {
                var worker = new Worker(this, options, random, i);
                var t = new Thread(() => worker.RunOneThread());
                threads.Add(t);
            }

            var sw = Stopwatch.StartNew();
            foreach (var t in threads)
                t.Start();
            foreach (var t in threads)
                t.Join();
            var timeMilli = sw.ElapsedMilliseconds;
            // TODO(Tianyu): More sophisticated output for automation
            Console.WriteLine(options.NumOps * options.NumThreads * 1000.0 / timeMilli);
        }
    }
}