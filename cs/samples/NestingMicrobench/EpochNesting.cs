using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace epvs
{
    internal class EpochNesting
    {
        internal List<SimpleVersionScheme> tested;
        internal List<BravoLatch> testedBravo;
        internal LightEpoch underlyingEpoch;
        internal byte[] hashBytes;

        internal class Worker
        {
            private byte[] scratchPad;
            private HashAlgorithm hasher;

            // private long scratchPad;
            private EpochNesting parent;
            private List<int> versionChangeIndexes;
            private int numOps, versionChangeDelay, numaStyle, threadId;
            private bool useBravo;

            internal Worker(EpochNesting parent, Options options, Random random, int threadId)
            {
                hasher = new SHA256Managed();
                scratchPad = new byte[hasher.HashSize / 8];
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

                Debug.Assert(options.SynchronizationMode.Equals("epvs-share") ||
                             options.SynchronizationMode.Equals("epvs-noshare") ||
                             options.SynchronizationMode.Equals("bravo"));
                useBravo = options.SynchronizationMode.Equals("bravo");
            }

            private void DoWork(int numUnits)
            {
                for (var i = 0; i < numUnits; i++)
                    hasher.TryComputeHash(parent.hashBytes, scratchPad, out _);
                // scratchPad++;
            }
            

            internal void RunOneThread()
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)threadId);
                else if (numaStyle == 1)
                    Native32.AffinitizeThreadShardedNuma((uint)threadId, 2); // assuming two NUMA sockets
                
                var nextChangeIndex = 0;
                for (var i = 0; i < numOps; i++)
                {
                    if (useBravo)
                    {
                        if (nextChangeIndex < versionChangeIndexes.Count &&
                            i == versionChangeIndexes[nextChangeIndex])
                        {
                            parent.testedBravo[i % parent.testedBravo.Count].EnterWriteLock();
                            DoWork(versionChangeDelay);
                            parent.testedBravo[i % parent.testedBravo.Count].ExitWriteLock();
                            nextChangeIndex++;
                        }
                        else
                        {
                            for (var j = 0; j < parent.testedBravo.Count; j++)
                                parent.testedBravo[j].EnterReadLock();
                            DoWork(1);
                            for (var j = parent.testedBravo.Count - 1; j > 0; j++)
                                parent.testedBravo[j].ExitReadLock();
                        }
                    }
                    else
                    {
                        if (nextChangeIndex < versionChangeIndexes.Count &&
                            i == versionChangeIndexes[nextChangeIndex])
                        {
                            parent.tested[i % parent.tested.Count].AdvanceVersion((_, _) => DoWork(versionChangeDelay));
                            nextChangeIndex++;
                        }
                        else
                        {
                            for (var j = 0; j < parent.testedBravo.Count; j++)
                                parent.tested[j].Enter();
                            DoWork(1);
                            for (var j = parent.testedBravo.Count - 1; j > 0; j++)
                                parent.tested[j].Leave();
                        }
                    }
                }
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[8];
            new Random().NextBytes(hashBytes);
            LightEpoch.InitializeStatic(512, 16);
            tested = new List<SimpleVersionScheme>();
            testedBravo = new List<BravoLatch>();
            underlyingEpoch = new LightEpoch();
            for (var i = 0; i < options.NumInstances; i++)
            {
                testedBravo.Add(new BravoLatch());
                tested.Add(new SimpleVersionScheme(options.SynchronizationMode.Equals("epvs-share") ? underlyingEpoch : new LightEpoch()));
            }

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
            var throughput = options.NumOps * options.NumThreads * 1000.0 / timeMilli;
            Console.WriteLine(throughput);
            if (!options.OutputFile.Equals(""))
            {
                using var outputFile = new StreamWriter(options.OutputFile, true);
                outputFile.WriteLine(throughput);
            }
        }
    }
}