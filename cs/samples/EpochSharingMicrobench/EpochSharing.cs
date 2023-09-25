using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace epvs
{
    internal class EpochSharing
    {
        internal List<EpochProtectedVersionScheme> tested;
        internal List<BravoLatch> testedBravo;
        internal List<ReaderWriterLockSlim> testedLatches;
        internal EpochProtectedVersionScheme sharedEpoch = new();
        internal BravoLatch sharedBravo = new();
        internal ReaderWriterLockSlim sharedLatch = new();
        internal byte[] hashBytes;

        internal class Worker
        {
            private byte[] scratchPad;
            private HashAlgorithm hasher;

            // private long scratchPad;
            private EpochSharing parent;
            private List<int> versionChangeIndexes;
            private int numOps, versionChangeDelay, numaStyle, threadId, index;
            private bool useBravo;
            private bool useLatch;

            internal Worker(EpochSharing parent, Options options, Random random, int threadId)
            {
                hasher = new SHA256Managed();
                scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                versionChangeIndexes = new List<int>();
                numOps = options.NumOps;
                versionChangeDelay = options.VersionChangeDelay;
                numaStyle = options.NumaStyle;
                this.threadId = threadId;
                index = threadId % parent.tested.Count;

                for (var i = 0; i < numOps; i++)
                {
                    if (random.NextDouble() < options.VersionChangeProbability)
                        versionChangeIndexes.Add(i);
                }
                
                useBravo = options.SynchronizationMode.StartsWith("bravo");
                useLatch = options.SynchronizationMode.StartsWith("latch");
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
                            parent.testedBravo[index].EnterWriteLock();
                            DoWork(versionChangeDelay);
                            parent.testedBravo[index].ExitWriteLock();
                            nextChangeIndex++;
                        }
                        else
                        {
                            var token = parent.testedBravo[index].EnterReadLock();
                            DoWork(1);
                            parent.testedBravo[index].ExitReadLock(token);
                        }
                    }
                    else if (useLatch)
                    {
                        if (nextChangeIndex < versionChangeIndexes.Count &&
                         i == versionChangeIndexes[nextChangeIndex])
                        {
                            parent.testedLatches[index].EnterWriteLock();
                            DoWork(versionChangeDelay);
                            parent.testedLatches[index].ExitWriteLock();
                            nextChangeIndex++;
                        }
                        else
                        {
                            parent.testedLatches[index].EnterReadLock();
                            DoWork(1);
                            parent.testedLatches[index].ExitReadLock();
                        }
                        
                    }
                    else
                    {
                        if (nextChangeIndex < versionChangeIndexes.Count &&
                            i == versionChangeIndexes[nextChangeIndex])
                        {
                            parent.tested[index].AdvanceVersion((_, _) => DoWork(versionChangeDelay));
                            nextChangeIndex++;
                        }
                        else
                        {
                            parent.tested[index].Enter();
                            DoWork(1);
                            parent.tested[index].Leave();
                        }
                    }
                }
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[8];
            new Random().NextBytes(hashBytes);
            tested = new List<EpochProtectedVersionScheme>();
            testedBravo = new List<BravoLatch>();
            testedLatches = new List<ReaderWriterLockSlim>();
            for (var i = 0; i < options.NumInstances; i++)
            {
                testedBravo.Add(options.SynchronizationMode.Equals("bravo-share") ? sharedBravo : new BravoLatch());
                tested.Add(options.SynchronizationMode.Equals("epvs-share") ? sharedEpoch :  new EpochProtectedVersionScheme());
                testedLatches.Add(options.SynchronizationMode.Equals("latch-share") ? sharedLatch :  new ReaderWriterLockSlim());
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