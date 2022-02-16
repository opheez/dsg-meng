using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
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
            private byte[] scratchPad;
            private HashAlgorithm hasher;
            private EpvsBench parent;
            private Queue<int> versionChangeIndexes;
            private int numOps, versionChangeDelay, numaStyle, threadId;

            internal Worker(EpvsBench parent, Options options, int threadId)
            {
                hasher = new SHA256Managed();
                scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                versionChangeIndexes = new Queue<int>();
                numOps = options.NumOps;
                versionChangeDelay = options.VersionChangeDelay;
                numaStyle = options.NumaStyle;
                this.threadId = threadId;

                var random = new Random();
                for (var i = 0; i < numOps; i++)
                {
                    if (random.NextDouble() < options.VersionChangeProbability)
                        versionChangeIndexes.Enqueue(i);
                }
            }

            private void DoWork(int numUnits)
            {
                for (var i = 0; i < numUnits; i++)
                    hasher.TryComputeHash(parent.hashBytes, scratchPad, out _);
            }

            internal void RunOneThread()
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint) threadId);
                else
                    Native32.AffinitizeThreadShardedNuma((uint) threadId, 2); // assuming two NUMA sockets

                for (var i = 0; i < numOps; i++)
                {
                    if (i == versionChangeIndexes.Peek())
                    {
                        versionChangeIndexes.Dequeue();
                        parent.tested.AdvanceVersion((_, _) => DoWork(versionChangeDelay));
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
            for (var i = 0; i < options.NumThreads; i++)
            {
                var worker = new Worker(this, options, i);
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
            Console.WriteLine(options.NumOps * options.NumThreads / (double) timeMilli);
        }
    }
}