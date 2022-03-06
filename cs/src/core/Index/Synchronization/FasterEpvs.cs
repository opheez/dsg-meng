using System;
using System.Threading.Tasks;

namespace FASTER.core
{
    public enum FasterEpvsPhase : byte
    {
        REST = VersionSchemeState.REST, LOG_FLUSH 
    }
    
    
    public class FasterEpvsStateMachine<Key, Value> : VersionSchemeStateMachine
    {
        private FasterKV<Key, Value> faster;
        private long lastVersion;
        
        public FasterEpvsStateMachine(FasterKV<Key, Value> faster, EpochProtectedVersionScheme epvs, long toVersion = -1) : base(epvs, toVersion)
        {
            this.faster = faster;
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            switch ((FasterEpvsPhase) currentState.Phase)
            {
                case FasterEpvsPhase.REST:
                    nextState = VersionSchemeState.Make((byte) FasterEpvsPhase.LOG_FLUSH, currentState.Version);
                    return true;
                case FasterEpvsPhase.LOG_FLUSH:
                    nextState = VersionSchemeState.Make((byte) FasterEpvsPhase.REST, actualToVersion);
                    return faster.hlog.FlushedUntilAddress >= faster._hybridLogCheckpoint.info.finalLogicalAddress;
                default:
                    throw new NotImplementedException();
            }
        }
        
        private static void CollectMetadata(VersionSchemeState next, FasterKV<Key, Value> faster)
        {
            // Collect object log offsets only after flushes
            // are completed
            var seg = faster.hlog.GetSegmentOffsets();
            if (seg != null)
            {
                faster._hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                Array.Copy(seg, faster._hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
            }

            // Temporarily block new sessions from starting, which may add an entry to the table and resize the
            // dictionary. There should be minimal contention here.
            lock (faster._activeSessions)
                // write dormant sessions to checkpoint
                foreach (var kvp in faster._activeSessions)
                    kvp.Value.AtomicSwitch(next.Version - 1);
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            switch ((FasterEpvsPhase) fromState.Phase)
            {
                case FasterEpvsPhase.REST:
                    lastVersion = faster.systemState.Version;
                    if (faster._hybridLogCheckpoint.IsDefault())
                    {
                        faster._hybridLogCheckpointToken = Guid.NewGuid();
                        faster.InitializeHybridLogCheckpoint(faster._hybridLogCheckpointToken, toState.Version);
                    }
                    faster._hybridLogCheckpoint.info.version = toState.Version;
                    faster.ObtainCurrentTailAddress(ref faster._hybridLogCheckpoint.info.startLogicalAddress);
                    faster._hybridLogCheckpoint.info.headAddress = faster.hlog.HeadAddress;
                    faster._hybridLogCheckpoint.info.beginAddress = faster.hlog.BeginAddress;
                    faster._hybridLogCheckpoint.info.nextVersion = toState.Version;
                    faster.hlog.ShiftReadOnlyToTail(out var tailAddress,
                        out faster._hybridLogCheckpoint.flushedSemaphore);
                    faster._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
                    break;
                case FasterEpvsPhase.LOG_FLUSH:
                    CollectMetadata(toState, faster);
                    // faster.WriteHybridLogMetaInfo();
                    faster.lastVersion = lastVersion;
                    faster._hybridLogCheckpoint.Dispose();
                    var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    faster.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                    faster.checkpointTcs = nextTcs;

                    break;
                default:
                    throw new NotImplementedException();
            }        
        }

        public override void AfterEnteringState(VersionSchemeState state)
        {
        }
    }
}