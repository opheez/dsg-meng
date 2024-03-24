using System.Diagnostics;
using FASTER.core;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using protobuf;
using Status = Grpc.Core.Status;

namespace ExampleServices.splog;

public class SpeculativeLog : StateObject
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


public class SplogService : protobuf.SplogService.SplogServiceBase
{
    private SpeculativeLog backend;

    public SplogService(SpeculativeLog backend)
    {
        this.backend = backend;
        backend.ConnectToCluster(out _);
    }

    public override Task<SplogAppendResponse> Append(SplogAppendRequest request, ServerCallContext context)
    {
        var lsn = backend.log.Enqueue(request.Entry.Span);
        return Task.FromResult(new SplogAppendResponse
        {
            Ok = true,
            Lsn = lsn
        });
    }

    private unsafe bool TryReadOneEntry(FasterLogScanIterator iterator, SplogScanResponse response)
    {
        if (!iterator.UnsafeGetNext(out var entry, out var entryLength, out var lsn, out var nextAddress)) return false;
        response.Entries.Add(new SplogEntry
        {
            Entry = ByteString.CopyFrom(new Span<byte>(entry, entryLength)),
            Lsn = lsn
        });
        response.NextLsn = nextAddress;
        iterator.UnsafeRelease();
        return true;
    }

    private async Task<bool> NextEntryWithTimeOut(FasterLogScanIterator iterator, int timeoutMilli, Stopwatch timer)
    {
        var currentTime = timer.ElapsedMilliseconds;
        if (currentTime > timeoutMilli) return false;
        var nextEntry = iterator.WaitAsync().AsTask();
        var session = backend.DetachFromWorker();
        var result =  await Task.WhenAny(nextEntry, Task.Delay((int)(timeoutMilli - currentTime)));
        if (!backend.TryMergeAndStartAction(session))
            throw new RpcException(Status.DefaultCancelled);
        return result == nextEntry;
    }

    public override async Task<SplogScanResponse> Scan(SplogScanRequest request, ServerCallContext context)
    {
        var responseObject = new SplogScanResponse();
        var scanner = backend.log.Scan(request.StartLsn, request.EndLsn, recover: false, scanUncommitted: true);
        var timer = Stopwatch.StartNew();
        for (var i = 0; i < request.MaxChunkSize; i++)
        {
            if (TryReadOneEntry(scanner, responseObject)) continue;
            if (!await NextEntryWithTimeOut(scanner, request.TimeoutMilli, timer)) break;
        }

        return responseObject;
    }

    public override Task<SplogTruncateResponse> Truncate(SplogTruncateRequest request, ServerCallContext context)
    {
        backend.log.TruncateUntil(request.NewStartLsn);
        return Task.FromResult(new SplogTruncateResponse
        {
            Ok = true
        });
    }
}