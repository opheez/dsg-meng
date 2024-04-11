using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// A DprWorker corresponds to an individual stateful failure domain (e.g., a physical machine or VM) in the system.
    /// DprWorkers have access to some external persistent storage and can commit and restore state through it using the
    /// StateObject implementation.
    /// </summary>
    /// <typeparam name="TStateObject"> type of state object</typeparam>
    public abstract class StateObject : IDisposable
    {
        private readonly SimpleObjectPool<LightDependencySet> dependencySetPool;
        public readonly DprWorkerOptions options;

        private readonly ConcurrentDictionary<long, LightDependencySet> versions;
        protected readonly IVersionScheme versionScheme;
        private long worldLine = 1;

        private long lastCheckpointMilli, lastRefreshMilli;
        private Stopwatch sw = Stopwatch.StartNew();

        private readonly byte[] depSerializationArray;
        private TaskCompletionSource<long> nextCommit;

        private List<IStateObjectAttachment> attachments = new List<IStateObjectAttachment>();
        private byte[] metadataBuffer = new byte[1 << 15];

        private SimpleObjectPool<DprSession> sessionPool;

        /// <summary>
        /// Creates a new DprServer.
        /// </summary>
        /// <param name="stateObject"> underlying state object </param>
        /// <param name="options"> DPR worker options </param>
        // TODO(Tianyu): Put some design work into the different operating modes of applications written this way -- speculative/pessimistic/no guarantees
        public StateObject(IVersionScheme versionScheme, DprWorkerOptions options)
        {
            this.options = options;
            this.versionScheme = versionScheme;
            
            versions = new ConcurrentDictionary<long, LightDependencySet>();
            dependencySetPool = new SimpleObjectPool<LightDependencySet>(() => new LightDependencySet());
            depSerializationArray = new byte[2 * LightDependencySet.MaxClusterSize * sizeof(long)];
            nextCommit = new TaskCompletionSource<long>();
            sessionPool = new SimpleObjectPool<DprSession>(() => new DprSession());
        }
        
        public IDprFinder GetDprFinder() => options.DprFinder; 

        /// <summary></summary>
        /// <returns> A task that completes when the next commit is recoverable</returns>
        public Task<long> NextCommit() => nextCommit.Task;

        /// <summary>
        /// Add the given attachment to the DprWorker. Should only be invoked before connecting to the cluster.
        /// </summary>
        /// <param name="attachment"> the attachment to add </param>
        public void AddAttachment(IStateObjectAttachment attachment)
        {
            attachments.Add(attachment);
        }

        /// <summary></summary>
        /// <returns> Worker ID of this DprServer instance </returns>
        public DprWorkerId Me() => options.Me;

        // TODO: The following two methods are technically only meaningful under protection
        /// <summary></summary>
        /// <returns> WorldLine of current DprWorker </returns>
        public long WorldLine() => worldLine;

        /// <summary></summary>
        /// <returns> Version of current DprWorker </returns>
        public long Version() => versionScheme.CurrentState().Version;

        private Task BeginRestore(long newWorldLine, long version)
        {
            var tcs = new TaskCompletionSource<object>();
            // Restoration to this particular worldline has already been completed
            if (worldLine >= newWorldLine) return Task.CompletedTask;

            versionScheme.TryAdvanceVersionWithCriticalSection((vOld, vNew) =>
            {
                // Restore underlying state object state
                RestoreCheckpoint(version, out var metadata);
                // Use the restored metadata to restore attachments state
                unsafe
                {
                    fixed (byte* src = metadata)
                    {
                        var head = src +
                                   SerializationUtil.DeserializeCheckpointMetadata(metadata, out _, out _, out _);
                        var numAttachments = *(int*)head;
                        head += sizeof(int);
                        Debug.Assert(numAttachments == attachments.Count,
                            "recovered checkpoint contains a different number of attachments!");
                        foreach (var attachment in attachments)
                        {
                            var size = *(int*)head;
                            head += sizeof(int);
                            attachment.RecoverFrom(new Span<byte>(head, size));
                            head += size;
                        }
                    }
                }

                // Clear any leftover state and signal complete
                versions.Clear();
                var deps = dependencySetPool.Checkout();
                if (vOld != 0)
                    deps.Update(options.Me, vOld);
                var success = versions.TryAdd(vNew, deps);
                Debug.Assert(success);
                tcs.SetResult(null);
                worldLine = newWorldLine;
            }, Math.Max(version, versionScheme.CurrentState().Version) + 1);

            return tcs.Task;
        }

        private int MetadataSize(ReadOnlySpan<byte> deps)
        {
            var result = deps.Length + sizeof(int);
            foreach (var attachment in attachments)
                result += sizeof(int) + attachment.SerializedSize();
            return result;
        }

        private bool BeginCheckpoint(long targetVersion = -1)
        {
            return versionScheme.TryAdvanceVersionWithCriticalSection((vOld, vNew) =>
            {
                // Prepare checkpoint metadata
                int length;
                var deps = ComputeCheckpointMetadata(vOld);
                Debug.Assert(MetadataSize(deps) < metadataBuffer.Length);
                unsafe
                {
                    fixed (byte* dst = metadataBuffer)
                    {
                        var head = dst;
                        var end = dst + metadataBuffer.Length;
                        deps.CopyTo(new Span<byte>(head, (int)(end - head)));
                        head += deps.Length;

                        *(int*)head = attachments.Count;
                        head += sizeof(int);

                        foreach (var attachment in attachments)
                        {
                            var size = attachment.SerializedSize();
                            *(int*)head = size;
                            head += sizeof(int);
                            attachment.SerializeTo(new Span<byte>(head, (int)(end - head)));
                            head += size;
                        }

                        length = (int)(head - dst);
                    }
                }

                // Perform checkpoint with a callback to report persistence and clean-up leftover tracking state
                PerformCheckpoint(vOld, new Span<byte>(metadataBuffer, 0, length), () =>
                {
                    versions.TryRemove(vOld, out var deps);
                    var workerVersion = new WorkerVersion(options.Me, vOld);
                    options.DprFinder?.ReportNewPersistentVersion(worldLine, workerVersion, deps);
                    dependencySetPool.Return(deps);
                });

                // Prepare new version before any operations can occur in it
                var newDeps = dependencySetPool.Checkout();
                if (vOld != 0) newDeps.Update(options.Me, vOld);
                var success = versions.TryAdd(vNew, newDeps);
                Debug.Assert(success);

            }, targetVersion) == StateMachineExecutionStatus.OK;
        }

        /// <summary>
        /// At the start (restart) of processing, connect to the rest of the DPR cluster. If the worker restarted from
        /// an existing instance, the cluster will detect this and trigger rollback as appropriate across the cluster,
        /// and the worker will automatically load the correct checkpointed state for recovery. Must be invoked exactly
        /// once before any other operations. 
        /// </summary>
        public void ConnectToCluster(out bool restored)
        {
            long versionToRecover = 0;
            if (options.DprFinder != null)
            {
                versionToRecover = options.DprFinder.AddWorker(options.Me, GetUnprunedVersions);
            }
            else
            {
                foreach (var v in GetUnprunedVersions())
                {
                    SerializationUtil.DeserializeCheckpointMetadata(v.Span, out _, out var wv, out _);
                    if (wv.Version > versionToRecover)
                        versionToRecover = wv.Version;
                }
            }

            // This worker is recovering from some failure and we need to load said checkpoint
            restored = versionToRecover != 0;
            if (restored)
                BeginRestore(options.DprFinder?.SystemWorldLine() ?? 1, versionToRecover).GetAwaiter().GetResult();
            else
            {
                var deps = dependencySetPool.Checkout();
                var success = versions.TryAdd(1, deps);
                Debug.Assert(success);
            }

            options.DprFinder?.Refresh(options.Me, GetUnprunedVersions);
        }

        private ReadOnlySpan<byte> ComputeCheckpointMetadata(long version)
        {
            var deps = versions[version];
            var size = SerializationUtil.SerializeCheckpointMetadata(depSerializationArray,
                worldLine, new WorkerVersion(options.Me, version), deps);
            Debug.Assert(size > 0);
            return new ReadOnlySpan<byte>(depSerializationArray, 0, size);
        }

        /// <summary></summary>
        /// <returns> Get the largest version number that is considered committed (will be recovered to) of this DPR Worker</returns>
        public long CommittedVersion()
        {
            return options.DprFinder?.SafeVersion(Me()) ?? Version() - 1;
        }

        public void Refresh()
        {
            var currentTime = sw.ElapsedMilliseconds;
            var lastCommitted = CommittedVersion();

            if (options.DprFinder != null && lastRefreshMilli + options.RefreshPeriodMilli < currentTime)
            {
                // A false return indicates that the DPR finder does not have a cut available, this is usually due to
                // restart from crash, at which point we should resend the graph 
                options.DprFinder.Refresh(options.Me, GetUnprunedVersions);
                core.Utility.MonotonicUpdate(ref lastRefreshMilli, currentTime, out _);
                if (worldLine != options.DprFinder.SystemWorldLine())
                    BeginRestore(options.DprFinder.SystemWorldLine(), options.DprFinder.SafeVersion(options.Me))
                        .GetAwaiter().GetResult();
            }

            if (lastCheckpointMilli + options.CheckpointPeriodMilli <= currentTime)
            {
                // TODO(Tianyu): Should avoid unnecessarily performing a checkpoint when underlying state object has not changed
                // TODO(Tianyu): Study when to fast-forward a version by more than one
                BeginCheckpoint(versionScheme.CurrentState().Version + 1);
                core.Utility.MonotonicUpdate(ref lastCheckpointMilli, currentTime, out _);
            }

            // Can prune dependency information of committed versions
            var newCommitted = CommittedVersion();
            if (lastCommitted != newCommitted)
            {
                var oldTask = nextCommit;
                nextCommit = new TaskCompletionSource<long>();
                oldTask.SetResult(newCommitted);
            }

            for (var i = lastCommitted; i < newCommitted; i++)
                PruneVersion(i);
        }

        public bool TryReceiveAndStartAction(ReadOnlySpan<byte> headerBytes, LightEpoch.EpochContext context = null)
        {
            // Should not be interacting with DPR-related things if speculation is disabled
            if (options.DprFinder == null) throw new InvalidOperationException();
            
            ref var header =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprMessageHeader>(headerBytes));

            // Apply the commit ordering rule, taking checkpoints if necessary.
            while (header.Version > versionScheme.CurrentState().Version)
            {
                // TODO(Tianyu): Should provide version that does not take checkpoints on the spot?
                if (BeginCheckpoint(header.Version))
                    Utility.MonotonicUpdate(ref lastCheckpointMilli, sw.ElapsedMilliseconds, out _);
                Thread.Yield();
            }

            // Enter protected region so the world-line does not shift while we determine whether a message is safe to consume
            versionScheme.Enter(context);
            // If the worker world-line is behind, wait for worker to recover up to the same point as the client,
            // so client operation is not lost in a rollback that the client has already observed.
            while (header.WorldLine > worldLine)
            {
                versionScheme.Leave(context);
                // TODO(Tianyu): Should provide version that does not rollback on the spot?
                BeginRestore(header.WorldLine, options.DprFinder.SafeVersion(options.Me)).GetAwaiter().GetResult();
                Thread.Yield();
                versionScheme.Enter(context);
            }

            // If the worker world-line is newer, the request must be dropped. 
            if (header.WorldLine != 0 && header.WorldLine < worldLine)
                return false;

            // Update batch dependencies to the current worker-version. This is an over-approximation, as the batch
            // could get processed at a future version instead due to thread timing. However, this is not a correctness
            // issue, nor do we lose much precision as batch-level dependency tracking is already an approximation.
            var deps = versions[versionScheme.CurrentState().Version];
            if (!header.SrcWorkerId.Equals(DprWorkerId.INVALID))
                deps.Update(header.SrcWorkerId, header.Version);
            unsafe
            {
                fixed (byte* d = header.data)
                {
                    var depsHead = d + header.ClientDepsOffset;
                    for (var i = 0; i < header.NumClientDeps; i++)
                    {
                        ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                        deps.Update(wv.DprWorkerId, wv.Version);
                        depsHead += sizeof(WorkerVersion);
                    }
                }
            }
            return true;
        }
        
        public unsafe bool TrySynchronizeAndStartAction(DprSession session, LightEpoch.EpochContext context = null)
        {
            // Should not be interacting with DPR-related things if speculation is disabled
            if (options.DprFinder == null) throw new InvalidOperationException();
            // TODO(Tianyu): optimize if necessary
            var headerBytes = stackalloc byte[120];
            var header = new Span<byte>(headerBytes, 120);
            var len = session.TagMessage(header);
            if (len < 0)
                // TODO(Tianyu): handle case where we run out of space
                throw new NotImplementedException();
            return TryReceiveAndStartAction(header, context);
        }

        public void StartLocalAction(LightEpoch.EpochContext context = null) => versionScheme.Enter(context);

        public void EndAction(LightEpoch.EpochContext context = null) => versionScheme.Leave(context);

        public int ProduceTagAndEndAction(Span<byte> outputHeaderBytes, LightEpoch.EpochContext context = null)
        {
            // Should not be interacting with DPR-related things if speculation is disabled
            if (options.DprFinder == null) throw new InvalidOperationException();
            
            if (outputHeaderBytes.Length < DprMessageHeader.FixedLenSize)
                return -DprMessageHeader.FixedLenSize;

            ref var outputHeader =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprMessageHeader>(outputHeaderBytes));

            outputHeader.SrcWorkerId = Me();
            outputHeader.WorldLine = worldLine;
            outputHeader.Version = versionScheme.CurrentState().Version;
            outputHeader.NumClientDeps = 0;
            EndAction(context);
            return DprMessageHeader.FixedLenSize;
        }

        public DprSession DetachFromWorker(LightEpoch.EpochContext context = null)
        {
            var session = sessionPool.Checkout();
            session.UnsafeReset(this);
            EndAction(context);
            return session;
        }

        public unsafe bool TryMergeAndStartAction(DprSession detachedSession, LightEpoch.EpochContext context = null)
        {
            // Should not be interacting with DPR-related things if speculation is disabled
            if (options.DprFinder == null) throw new InvalidOperationException();
            var headerBytes = stackalloc byte[120];
            var header = new Span<byte>(headerBytes, 120);
            var len = detachedSession.TagMessage(header);
            if (len < 0)
                // TODO(Tianyu): handle case where we run out of space
                throw new NotImplementedException();
            sessionPool.Return(detachedSession);
            // TODO(Tianyu): optimize if necessary
            return TryReceiveAndStartAction(header, context);
        }

        /// <summary>
        ///     Force the execution of a checkpoint ahead of the schedule specified at creation time.
        ///     Resets the checkpoint schedule to happen checkpoint_milli after this invocation.
        /// </summary>
        /// <param name="targetVersion"> the version to jump to after the checkpoint, or -1 for the immediate next version</param>
        public void ForceCheckpoint(long targetVersion = -1)
        {
            if (BeginCheckpoint(targetVersion))
                core.Utility.MonotonicUpdate(ref lastCheckpointMilli, sw.ElapsedMilliseconds, out _);
        }

        public void ForceRefresh()
        {
            if (options.DprFinder == null) return;
            options.DprFinder.Refresh(options.Me, GetUnprunedVersions);
            core.Utility.MonotonicUpdate(ref lastRefreshMilli, sw.ElapsedMilliseconds, out _);
            if (worldLine != options.DprFinder.SystemWorldLine())
                BeginRestore(options.DprFinder.SystemWorldLine(), options.DprFinder.SafeVersion(options.Me))
                    .GetAwaiter().GetResult();
        }
        
        /// <summary>
        /// Performs a checkpoint uniquely identified by the given version number along with the given metadata to be
        /// persisted. Implementers are allowed to return as soon as the checkpoint content is finalized, but before
        /// it is persistent, but must invoke onPersist afterwards. LibDPR will ensure that this function does not
        /// interleave with protected processing logic, other checkpoint requests, or restores. 
        /// </summary>
        /// <param name="version"> A monotonically increasing unique ID describing this checkpoint </param>>
        /// <param name="metadata"> Any metadata, in bytes, to be persisted along with the checkpoint </param>
        /// <param name="onPersist"> Callback to invoke when checkpoint is persistent </param>
        public abstract void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist);

        /// <summary>
        /// Recovers to a previous checkpoint as identified by the version number, along with any metadata. The function
        /// returns only after the state is restored for all future calls. LibDPR will not interleave batch operation,
        /// other checkpoint requests, or restore requests with this function.
        /// </summary>
        /// <param name="version"> unique ID for the checkpoint to recover </param>>
        /// <param name="metadata"> Any metadata, in bytes, persisted along with the checkpoint </param>
        public abstract void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata);

        /// <summary>
        /// Removes a version from persistent storage. This method is only invoked when a version will no longer be
        /// recovered to.
        /// </summary>
        /// <param name="version"> unique ID for the checkpoint to remove </param>
        public abstract void PruneVersion(long version);

        /// <summary>
        /// Retrieves information about all unpruned checkpoints on persistent storage, along with persisted metadata. 
        /// </summary>
        /// <returns>
        /// enumerable of bytes that denotes the metadata of each unpruned checkpoint
        /// </returns>
        public abstract IEnumerable<Memory<byte>> GetUnprunedVersions();

        public abstract void Dispose();
    }
}