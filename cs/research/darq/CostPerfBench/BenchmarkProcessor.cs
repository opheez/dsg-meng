using System;
using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.libdpr;

namespace microbench
{
    public class BenchmarkProcessor : IDarqProcessor
    {
        private IDarqProcessorClientCapabilities capabilities;
        private WorkerId me, other;
        private StepRequest request;
        private int numSteps;

        public BenchmarkProcessor(WorkerId me, WorkerId other, int numSteps)
        {
            this.me = me;
            this.other = other;
            request = new StepRequest(null);
            this.numSteps = numSteps;
        }

        public unsafe bool ProcessMessage(DarqMessage m)
        {
            int numTasksLeft = BitConverter.ToInt32(m.GetMessageBody());
            if (numTasksLeft == 0)
            {
                m.Dispose();
                return false;
            }

            if (numTasksLeft % 100 == 0)
                Console.WriteLine($"{numTasksLeft} messages left to process");

            ParallelPi(numSteps);
            Span<byte> newMessage = stackalloc byte[sizeof(int)];
            BitConverter.TryWriteBytes(newMessage, --numTasksLeft);
            
            capabilities.Step(new StepRequestBuilder(request, me)
                .MarkMessageConsumed(m.GetLsn())
                .AddOutMessage(other, newMessage)
                .FinishStep());
            m.Dispose();
            return numTasksLeft != 0;
        }

        public void OnRestart(IDarqProcessorClientCapabilities capabilities)
        {
            this.capabilities = capabilities;
        }
        
        static double ParallelPi(int numSteps)
        {
            double sum = 0.0;
            double step = 1.0 / numSteps;
            object monitor = new object();
            Parallel.For(0, numSteps, () => 0.0, (i, _, local) =>
            {
                double x = (i + 0.5) * step;
                return local + 4.0 / (1.0 + x * x);
            }, local =>
            {
                lock (monitor) sum += local;
            });
            return step * sum;
        }
    }
}