using FASTER.core;
using FASTER.libdpr;

namespace ExampleServices.splog;

public class SpeculativeLog : DprWorker
{
    private FasterLogSettings settings;
    public FasterLog log;
    
    public SpeculativeLog(FasterLogSettings settings, IVersionScheme versionScheme, DprWorkerOptions options) : base(versionScheme, options)
    {
        this.settings = settings;
        log = new FasterLog(settings);
    }

    public override void Dispose()
    {
        log.Dispose();
    }
    
    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        log.CommitStrongly(out _, out _, false, metadata.ToArray(), version, onPersist);
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        log = new FasterLog(settings);
        log.Recover(version);
        metadata = log.RecoveredCookie;
    }

    public override void PruneVersion(long version)
    {
        settings.LogCommitManager.RemoveCommit(version);
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        var commits = settings.LogCommitManager.ListCommits().ToList();
        return commits.Select(commitNum =>
        {
            // TODO(Tianyu): hacky
            var newLog = new FasterLog(settings);
            newLog.Recover(commitNum);
            var commitCookie = newLog.RecoveredCookie;
            newLog.Dispose();
            return new Memory<byte>(commitCookie);
        });    
    }
}