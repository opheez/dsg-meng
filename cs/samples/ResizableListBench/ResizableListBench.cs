using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.core;

namespace epvs
{
    internal enum OperationType : byte
    {
        PUSH, READ, WRITE
    }
    internal class ResizableListBench<ListType> where ListType : IResizableList
    {
        internal ListType tested;

        internal ResizableListBench(ListType tested)
        {
            this.tested = tested;
        }

        internal class Worker<ListType> where ListType : IResizableList
        {
            private List<(OperationType, int, bool)> ops;
            private ResizableListBench<ListType> parent;
            private int numaStyle, threadId;
            internal List<long> pushLatencies, writeLatencies, readLatencies;

            internal Worker(ResizableListBench<ListType> parent, Options options, Random random, int threadId)
            {
                this.parent = parent;
                ops = new List<(OperationType, int, bool)>();
                for (var i = 0; i < options.NumOps; i++)
                {
                    OperationType type;
                    var typeToss = random.NextDouble();
                    if (typeToss < options.PushProbability)
                        type = OperationType.PUSH;
                    else if (typeToss < options.PushProbability + options.WriteProbability)
                        type = OperationType.WRITE;
                    else
                        type = OperationType.READ;
                    var index = random.Next();
                    var sampleToss = random.NextDouble();
                    ops.Add((type, index, sampleToss < options.LatencySampleRate));
                }

                numaStyle = options.NumaStyle;
                this.threadId = threadId;
                pushLatencies = new List<long>();
                writeLatencies = new List<long>();
                readLatencies = new List<long>();
            }

            internal void RunOneThread()
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint) threadId);
                else
                    Native32.AffinitizeThreadShardedNuma((uint) threadId, 2); // assuming two NUMA sockets

                var sw = Stopwatch.StartNew();
                foreach (var (type, index, sample) in ops)
                {
                    long tickStart = 0;
                    if (sample)
                        tickStart = sw.ElapsedTicks;
                    switch (type)
                    {
                        case OperationType.PUSH:
                            parent.tested.Push(0xDEADBEEF);
                            if (sample)
                                pushLatencies.Add(sw.ElapsedTicks - tickStart);
                            break;
                        case OperationType.READ:
                            parent.tested.Read(index % parent.tested.Count());
                            if (sample)
                                readLatencies.Add(sw.ElapsedTicks - tickStart);
                            break;
                        case OperationType.WRITE:
                            parent.tested.Write(index % parent.tested.Count(), 0xC0FFEE);
                            if (sample)
                                writeLatencies.Add(sw.ElapsedTicks - tickStart);
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
            }
        }

        private void ComputeStats(List<long> ticks, string tag)
        {
            if (ticks.Count == 0)
            {
                Console.WriteLine($"{tag} no measurements");
                return;
            }
            
            var avg = ticks.Average() * 1000000 / Stopwatch.Frequency;
            var std = Math.Sqrt(ticks.Sum(e => Math.Pow(e - avg, 2)) / ticks.Count) * 1000000 / Stopwatch.Frequency;

            ticks.Sort();
            var index = (int) Math.Ceiling(0.99 * (ticks.Count - 1));
            var p99 = ticks[index] * 1000000 / Stopwatch.Frequency;
            Console.WriteLine($"{tag} avg: {avg} us, std : {std} us, p99: {p99}" );
        }

        internal void RunExperiment(Options options)
        {
            var workers = new List<Worker<ListType>>();
            var threads = new List<Thread>();
            var random = new Random();
            for (var i = 0; i < options.NumThreads; i++)
            {
                var worker = new Worker<ListType>(this, options, random, i);
                workers.Add(worker);
                var t = new Thread(() => worker.RunOneThread());
                threads.Add(t);
            }

            var sw = Stopwatch.StartNew();
            // Start off with a few entries
            for (var i = 0; i < 128; i++)
                tested.Push(0xDEADBEEF);

            foreach (var t in threads)
                t.Start();
            foreach (var t in threads)
                t.Join();
            var timeMilli = sw.ElapsedMilliseconds;
            // TODO(Tianyu): More sophisticated output for automation
            Console.WriteLine(options.NumOps * options.NumThreads * 1000.0 / timeMilli);
            
            ComputeStats(workers.SelectMany(w => w.pushLatencies).ToList(), "Push");
            ComputeStats(workers.SelectMany(w => w.readLatencies).ToList(), "Read");
            ComputeStats(workers.SelectMany(w => w.writeLatencies).ToList(), "Write");

        }
    }
}