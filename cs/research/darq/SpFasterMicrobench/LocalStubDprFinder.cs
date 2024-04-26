using FASTER.libdpr;

namespace microbench;

public class LocalStubDprFinder : IDprFinder
{
    private long persistedVersion;
    
    public long SafeVersion(DprWorkerId dprWorkerId)
    {
        return persistedVersion;
    }

    public long SystemWorldLine()
    {
        return 1;
    }

    public void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
    {
        persistedVersion = persisted.Version;
    }

    public void Refresh(DprWorkerId id, IDprFinder.UnprunedVersionsProvider provider)
    {
    }

    public void RefreshStateless()
    {
    }

    public long AddWorker(DprWorkerId id, IDprFinder.UnprunedVersionsProvider provider)
    {
        return 0;
    }

    public void RemoveWorker(DprWorkerId id)
    {
    }
}