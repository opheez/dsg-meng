using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace EpvsMicrobench
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
            private int numOps, versionChangeDelay;

            internal Worker(EpvsBench parent, Options options)
            {
                hasher = new SHA256Managed();
                scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                versionChangeIndexes = new Queue<int>();
                numOps = options.NumOps;
                versionChangeDelay = options.VersionChangeDelay;
                
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


        internal void RunExperiment( Options options)
        {
            hashBytes = new byte[16];
            new Random().NextBytes(hashBytes);
            LightEpoch.InitializeStatic(options.EpochTableSize, options.DrainListSize);
            var tested = new SimpleVersionScheme();

            var threads = new List<Thread>();
            for (var i = 0; i < options.NumThreads; i++)
            {
                var worker = new Worker(this, options);
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
            Console.WriteLine(timeMilli);
        }
        
    }
}