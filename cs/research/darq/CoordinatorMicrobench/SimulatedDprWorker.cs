using System.Diagnostics;
using FASTER.libdpr;

namespace microbench;

public class SimulatedDprWorker
{
    private IDprFinder dprFinder;
    private IWorkloadGenerator generator;
    private List<DprWorkerId> workers;
    private List<DprWorkerId> toSimulate;

    private Dictionary<WorkerVersion, long> versionPersistent = new(), versionRecoverable = new();
    private Stopwatch stopwatch;

    public SimulatedDprWorker(IDprFinder dprFinder, IWorkloadGenerator generator,
        List<DprWorkerId> workers, List<DprWorkerId> toSimulate)
    {
        foreach (var w in toSimulate)
            dprFinder.AddWorker(w, Enumerable.Empty<Memory<byte>>);
        this.dprFinder = dprFinder;
        this.generator = generator;
        this.workers = workers;
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
        var currentVersion = 1L;
        var safeVersions = new Dictionary<DprWorkerId, long>();
        foreach (var w in toSimulate)
            safeVersions[w] = 0;
        stopwatch.Start();
        while (stopwatch.ElapsedMilliseconds < runSeconds * 1000)
        {
            var elapsed = stopwatch.ElapsedMilliseconds;
            var currentTime = stopwatch.ElapsedTicks;
            dprFinder.RefreshStateless();
            foreach (var w in toSimulate)
            {
                var currentSafeVersion = dprFinder.SafeVersion(w);
                for (var i = safeVersions[w] + 1; i <= currentSafeVersion; i++)
                    versionRecoverable.Add(new WorkerVersion(w, i), currentTime);
                safeVersions[w] = currentSafeVersion;
            }

            var expectedVersion = 1 + elapsed / checkpointMilli;
            if (expectedVersion > currentVersion)
            {
                foreach (var w in toSimulate)
                {
                    var wv = new WorkerVersion(w, currentVersion);
                    var deps = generator.GenerateDependenciesOneRun(workers, w, currentVersion);
                    versionPersistent.Add(wv, currentTime);
                    Task.Run(() => dprFinder.ReportNewPersistentVersion(1, wv, deps));
                }

                currentVersion = expectedVersion;
            }
        }
    }
}