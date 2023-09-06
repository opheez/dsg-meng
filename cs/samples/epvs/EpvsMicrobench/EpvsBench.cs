using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using CommandLine;
using FASTER.core;

namespace epvs
{
    internal class EpvsBench
    {
        internal SemaphoreSlim testedLatch;
        internal SimpleVersionScheme tested;
        internal byte[] hashBytes;

        internal class Worker
        {
            private byte[] scratchPad;

            private HashAlgorithm hasher;

            // private long scratchPad;
            private EpvsBench parent;
            private List<int> versionChangeIndexes;
            private int numOps, versionChangeDelay, numaStyle, threadId;

            private byte syncMode;

            internal Worker(EpvsBench parent, Options options, Random random, int threadId)
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

                switch (options.SynchronizationMode)
                {
                    case "latch-free":
                        syncMode = 0;
                        break;
                    case "epvs":
                        syncMode = 1;
                        break;
                    case "epvs-refresh":
                        syncMode = 2;
                        break;
                    case "latch":
                        syncMode = 3;
                        break;
                }
                
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
                    Native32.AffinitizeThreadRoundRobin((uint) threadId);
                else if (numaStyle == 1)
                    Native32.AffinitizeThreadShardedNuma((uint) threadId, 2); // assuming two NUMA sockets

                if (syncMode == 2)
                    parent.tested.Enter();
                
                var nextChangeIndex = 0;
                for (var i = 0; i < numOps; i++)
                {
                    switch (syncMode)
                    {
                        case 0:
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                DoWork(versionChangeDelay);
                                nextChangeIndex++;
                            }
                            else
                            {
                                DoWork(1);
                            }
                            break;
                        case 1:
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
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
                            break;
                        case 2:
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                while (parent.tested.TryAdvanceVersion((_, _) =>
                                {
                                    DoWork(versionChangeDelay);
                                }) == StateMachineExecutionStatus.RETRY)
                                    parent.tested.Refresh();
                                nextChangeIndex++;
                            }
                            else
                            {
                                parent.tested.Refresh();
                                DoWork(1);
                            }

                            break;
                        case 3:
                            parent.testedLatch.Wait();
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                DoWork(versionChangeDelay);
                                nextChangeIndex++;
                            }
                            else
                            {
                                DoWork(1);
                            }
                            parent.testedLatch.Release();
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
                
                if (syncMode == 2)
                    parent.tested.Leave();
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[8];
            new Random().NextBytes(hashBytes);
            LightEpoch.InitializeStatic(options.EpochTableSize, options.DrainListSize);
            tested = new SimpleVersionScheme();
            testedLatch = new SemaphoreSlim(1, 1);
            
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