using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.libdpr;

namespace microbench
{
    public unsafe class BenchmarkProcessor : IDarqProcessor
    {
        private IDarqProcessorClientCapabilities capabilities;
        private List<BlockingCollection<DarqMessage>> workerQueues;
        private List<Thread> threads;
        private CountdownEvent countdownEvent;

        public BenchmarkProcessor(WorkerId me, int numTenants, int numSteps)
        {
            workerQueues = new List<BlockingCollection<DarqMessage>>();
            threads = new List<Thread>();
            countdownEvent = new CountdownEvent(numTenants);

            for (var i = 0; i < numTenants; i++)
            {
                var collection = new BlockingCollection<DarqMessage>(1);
                workerQueues.Add(collection);
                var thread = new Thread(() =>
                {
                    Span<byte> buf = stackalloc byte[8];
                    StepRequest request = new StepRequest(null);

                    while (true)
                    {
                        var m = collection.Take();
                        var partitionId = BitConverter.ToInt32(m.GetMessageBody());
                        var numTasksLeft = BitConverter.ToInt32(m.GetMessageBody().Slice(sizeof(int)));

                        if (numTasksLeft % 10000 == 0)
                            Console.WriteLine($"{numTasksLeft} messages left to process");

                        SerialPi(numSteps);
                        BitConverter.TryWriteBytes(buf, partitionId);
                        BitConverter.TryWriteBytes(buf.Slice(sizeof(int)), --numTasksLeft);
                        capabilities.Step(new StepRequestBuilder(request, me)
                            .MarkMessageConsumed(m.GetLsn())
                            .AddOutMessage(me, buf)
                            .FinishStep());
                        m.Dispose();

                        if (numTasksLeft == 0) break;
                    }
                });
                threads.Add(thread);
                thread.Start();
            }
        }

        public bool ProcessMessage(DarqMessage m)
        {
            var messageBody = m.GetMessageBody();
            int partitionId = BitConverter.ToInt32(messageBody);
            int numTasksLeft = BitConverter.ToInt32(messageBody.Slice(sizeof(int), sizeof(int)));
            var collection = workerQueues[partitionId];
            if (numTasksLeft == 0)
            {
                m.Dispose();
                threads[partitionId].Join();
                return !countdownEvent.Signal();
            }

            var success = collection.TryAdd(m);
            Debug.Assert(success);
            return true;
        }

        public void OnRestart(IDarqProcessorClientCapabilities capabilities)
        {
            this.capabilities = capabilities;
        }

        static double SerialPi(int numSteps)
        {
            double sum = 0.0;
            double step = 1.0 / numSteps;
            for (int i = 0; i < numSteps; i++)
            {
                double x = (i + 0.5) * step;
                sum += 4.0 / (1.0 + x * x);
            }

            return step * sum;
        }
    }
}