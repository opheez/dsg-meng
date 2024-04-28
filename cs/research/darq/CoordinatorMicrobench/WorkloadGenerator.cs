using FASTER.libdpr;

namespace microbench;

public interface IWorkloadGenerator
{
    List<WorkerVersion> GenerateDependenciesOneRun(IList<DprWorkerId> workers, DprWorkerId me, long currentVersion);
}

public class UniformWorkloadGenerator : IWorkloadGenerator
{
    private List<WorkerVersion> dependecies = new();
    private Random rand = new();
    private double depProb;

    public UniformWorkloadGenerator(double depProb)
    {
        this.depProb = depProb;
    }

    public List<WorkerVersion> GenerateDependenciesOneRun(IList<DprWorkerId> workers, DprWorkerId me,
        long currentVersion)
    {
        dependecies.Clear();
        if (currentVersion != 1)
            dependecies.Add(new WorkerVersion(me, currentVersion - 1));
        foreach (var worker in workers)
        {
            if (worker.Equals(me)) continue;

            if (rand.NextDouble() < depProb)
                dependecies.Add(new WorkerVersion(worker, currentVersion));
        }

        return dependecies;
    }
}