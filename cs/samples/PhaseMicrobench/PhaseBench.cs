using System.Diagnostics;
using System.Security.Cryptography;
using FASTER.core;

namespace epvs
{
    internal class PhaseBenchmarkStateMachine : VersionSchemeStateMachine
    {
        private byte[] hashBytes;
        private int numPhases, workUnits;
        private byte[] scratchPad;
        private HashAlgorithm hasher;

        public PhaseBenchmarkStateMachine(EpochProtectedVersionScheme epvs, byte[] hashBytes, int numPhases,
            int workUnits,
            long toVersion = -1)
            : base(epvs, toVersion)
        {
            this.numPhases = numPhases;
            this.workUnits = workUnits;
            this.hashBytes = hashBytes;
            hasher = new SHA256Managed();
            scratchPad = new byte[hasher.HashSize / 8];
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            if (currentState.Phase == numPhases - 1)
                nextState = VersionSchemeState.Make(VersionSchemeState.REST, currentState.Version + 1);
            else
                nextState = VersionSchemeState.Make((byte)(currentState.Phase + 1), currentState.Version);
            return true;
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            DoWork(workUnits / numPhases);
        }

        public override void AfterEnteringState(VersionSchemeState state)
        {
        }

        private void DoWork(int numUnits)
        {
            for (var i = 0; i < numUnits; i++)
                hasher.TryComputeHash(hashBytes, scratchPad, out _);
        }
    }

    internal class EpvsBench
    {
        internal EpochProtectedVersionScheme tested;
        internal byte[] hashBytes;

        internal class Worker
        {
            private byte[] scratchPad;
            private HashAlgorithm hasher;
            private EpvsBench parent;
            private byte[] opIndexes;
            private int threadId;
            private Options options;

            internal Worker(EpvsBench parent, Options options, Random random, int threadId)
            {
                hasher = new SHA256Managed();
                scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                opIndexes = new byte[options.NumOps];
                this.options = options;
                this.threadId = threadId;

                for (var i = 0; i < options.NumOps; i++)
                {
                    if (random.NextDouble() < options.VersionChangeProbability)
                        opIndexes[i] = 2;
                    if (random.NextDouble() < options.BlockProbability)
                        opIndexes[i] = 1;
                    opIndexes[i] = 0;
                }
            }

            private void DoWork(int numUnits)
            {
                for (var i = 0; i < numUnits; i++)
                    hasher.TryComputeHash(parent.hashBytes, scratchPad, out _);
            }

            internal void RunOneThread()
            {
                // if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)threadId);
                // else if (numaStyle == 1)
                // Native32.AffinitizeThreadShardedNuma((uint)threadId, 2); // assuming two NUMA sockets

                parent.tested.Enter();

                for (var i = 0; i < options.NumOps; i++)
                {
                    switch (opIndexes[i])
                    {
                        case 0:
                            parent.tested.Refresh();
                            DoWork(1);
                            break;
                        case 1:
                            VersionSchemeState state;
                            do
                            {
                                state = parent.tested.Refresh();
                            } while (state.Phase != 0);
                            DoWork(1);
                            break;
                        case 2:
                            while (parent.tested.TryExecuteStateMachine(new PhaseBenchmarkStateMachine(parent.tested,
                                       parent.hashBytes, options.NumPhases, options.VersionChangeDelay)) ==
                                   StateMachineExecutionStatus.RETRY)
                                parent.tested.Refresh();
                            break;
                    }
                }

                parent.tested.Leave();
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[8];
            new Random().NextBytes(hashBytes);
            LightEpoch.InitializeStatic(options.EpochTableSize, options.DrainListSize);
            tested = new EpochProtectedVersionScheme();

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