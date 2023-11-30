using System.Collections.Generic;
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

        public DprStatus CheckStatus(long worldLine, WorkerVersion wv)
        {
            var state = frontState;
            if (worldLine < state.currentWorldLine && state.worldLinePrefix[wv.WorkerId] < wv.Version)
                return DprStatus.ROLLEDBACK;
            if (SafeVersion(wv.WorkerId) > wv.Version)
                return DprStatus.COMMITTED;
            return DprStatus.SPECULATIVE;
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