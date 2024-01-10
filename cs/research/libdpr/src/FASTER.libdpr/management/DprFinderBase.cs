using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.libdpr.proto;

namespace FASTER.libdpr
{
    public abstract class DprFinderBase : IDprFinder
    {
        // We maintain two cuts that alternate being updated, and atomically swap them
        private Dictionary<WorkerId, long> frontCut, backCut;
        private ClusterState frontState, backState;

        protected DprFinderBase()
        {
            frontCut = new Dictionary<WorkerId, long>();
            backCut = new Dictionary<WorkerId, long>();
            frontState = new ClusterState();
            backState = new ClusterState();
        }

        public long SafeVersion(WorkerId workerId)
        {
            return frontCut.TryGetValue(workerId, out var result) ? result : 0;
        }

        public long SystemWorldLine()
        {
            return frontState.currentWorldLine;
        }

        public DprStatus CheckStatus(ReadOnlySpan<byte> header)
        {
            ref readonly var dprHeader = ref MemoryMarshal.AsRef<DprMessageHeader>(header);
            var state = frontState;

            if (dprHeader.WorldLine < state.currentWorldLine) return DprStatus.ROLLEDBACK;
            if (dprHeader.SrcWorkerId == WorkerId.INVALID)
            {
                // This is a client dependency that uses the varlen dependency fields, so we need to check that those
                // are all committed instead
                unsafe
                {
                    fixed (byte* d = dprHeader.data)
                    {
                        var depsHead = d + dprHeader.ClientDepsOffset;
                        for (var i = 0; i < dprHeader.NumClientDeps; i++)
                        {
                            ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                            if (SafeVersion(dprHeader.SrcWorkerId) < dprHeader.Version) 
                                return DprStatus.SPECULATIVE;
                            depsHead += sizeof(WorkerVersion);
                        }

                        return DprStatus.COMMITTED;
                    }
                }

            }
            // Otherwise just check the originating worker
            return SafeVersion(dprHeader.SrcWorkerId) >= dprHeader.Version ? DprStatus.COMMITTED : DprStatus.SPECULATIVE;
        }

        public abstract void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted,
            IEnumerable<WorkerVersion> deps);

        protected abstract bool Sync(ClusterState stateToUpdate, Dictionary<WorkerId, long> cutToUpdate);

        protected abstract void SendGraphReconstruction(WorkerId id, IStateObject stateObject);

        protected abstract void AddWorkerInternal(WorkerId id);

        public abstract void RemoveWorker(WorkerId id);

        public void Refresh(WorkerId id, IStateObject stateObject)
        {
            // Reset data structures
            backCut.Clear();
            backState.currentWorldLine = 1;
            backState.worldLinePrefix.Clear();

            if (!Sync(backState, backCut))
            {
                SendGraphReconstruction(id, stateObject);
                Refresh(id, stateObject);
            }

            // Ok to not update the two atomically because cuts are resilient to cluster state changes anyway
            backState = Interlocked.Exchange(ref frontState, backState);
            backCut = Interlocked.Exchange(ref frontCut, backCut);
        }

        public void RefreshStateless()
        {
            backCut.Clear();
            // Cut is unavailable, do nothing.
            if (!Sync(backState, backCut)) return;

            // Ok to not update the two atomically because cuts are resilient to cluster state changes anyway
            backState = Interlocked.Exchange(ref frontState, backState);
            backCut = Interlocked.Exchange(ref frontCut, backCut);
        }

        public long AddWorker(WorkerId id, IStateObject stateObject)
        {
            // A blind resending of graph is necessary, in case the coordinator is undergoing recovery and pausing
            // processing of new workers until every worker has responded
            SendGraphReconstruction(id, stateObject);

            AddWorkerInternal(id);

            // Get cluster state afterwards to see if recovery is necessary
            Refresh(id, stateObject);
            return SafeVersion(id);
        }
    }
}