using System.Diagnostics;
using FASTER.libdpr;

namespace microbench;

 public class SimulatedDprWorker
    {
        private IDprFinder dprFinder;
        private IWorkloadGenerator generator;
        private IList<DprWorkerId> workers;
        private DprWorkerId me;

        private Dictionary<long, long> versionPersistent, versionRecoverable;
        private Stopwatch stopwatch;
        
        public SimulatedDprWorker(IDprFinder dprFinder, IWorkloadGenerator generator, 
            IList<DprWorkerId> workers, DprWorkerId me, Stopwatch stopwatch)
        {
            dprFinder.AddWorker(me, Enumerable.Empty<Memory<byte>>);
            this.dprFinder = dprFinder;
            this.generator = generator;
            this.workers = workers;
            this.me = me;
            this.stopwatch = stopwatch;
        }

        public List<long> ComputeVersionCommitLatencies()
        {
            var result = new List<long>(versionRecoverable.Count);

            foreach (var entry in versionRecoverable)
            {
                if (!versionPersistent.TryGetValue(entry.Key, out var startTime)) continue;
                result.Add(entry.Value - startTime);
            }

            return result;
        }

        public void RunContinuously(int runSeconds, int checkpointMilli)
        {
            versionPersistent = new Dictionary<long, long>();
            versionRecoverable = new Dictionary<long, long>();
            var currentVersion = 1L;
            var safeVersion = 0L;
            while (stopwatch.ElapsedMilliseconds < runSeconds * 1000)
            {
                var elapsed = stopwatch.ElapsedMilliseconds;
                var currentTime = stopwatch.ElapsedTicks;
                dprFinder.RefreshStateless();
                var currentSafeVersion = dprFinder.SafeVersion(me);
                for (var i = safeVersion + 1; i <= currentSafeVersion; i++)
                    versionRecoverable.Add(i, currentTime);

                safeVersion = currentSafeVersion;
                
                var expectedVersion = 1 + elapsed / checkpointMilli;
                if (expectedVersion > currentVersion)
                {
                    var deps = generator.GenerateDependenciesOneRun(workers, me, currentVersion);
                    versionPersistent.Add(currentVersion, currentTime);
                    dprFinder.ReportNewPersistentVersion(1, new WorkerVersion(me, currentVersion), deps);
                    currentVersion = expectedVersion;
                }
                Thread.Sleep(5);
            }
        }
    }