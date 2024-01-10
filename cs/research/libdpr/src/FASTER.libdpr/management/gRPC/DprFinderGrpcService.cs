using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.libdpr.proto;
using Grpc.Core;

namespace FASTER.libdpr
{
    public class GrpcPrecomputedSyncResponse : PrecomputedSyncResponseBase
    {
        internal SyncResponse obj = new SyncResponse();
        public override void ResetClusterState(ClusterState clusterState)
        {
            rwLatch.EnterWriteLock();
            obj.WorldLine = clusterState.currentWorldLine;
            obj.WorldLinePrefix.Clear();
            foreach (var entry in clusterState.worldLinePrefix)
                obj.WorldLinePrefix.Add(new proto.WorkerVersion
                {
                    Id = entry.Key.guid,
                    Version = entry.Value
                });
            rwLatch.ExitReadLock();
        }

        public override void UpdateCut(Dictionary<WorkerId, long> newCut)
        {
            rwLatch.EnterWriteLock();
            obj.CurrentCut.Clear();
            foreach (var entry in newCut)
                obj.CurrentCut.Add(new proto.WorkerVersion
                {
                    Id = entry.Key.guid,
                    Version = entry.Value
                });
            rwLatch.ExitWriteLock();
        }
    }
    
    public class DprFinderGrpcService : DprFinder.DprFinderBase, IDisposable
    {
        private readonly GraphDprFinderBackend backend;
        private Thread processThread;
        private ManualResetEventSlim termination;
        private GrpcPrecomputedSyncResponse response;
        
        public DprFinderGrpcService(GraphDprFinderBackend backend)
        {
            this.backend = backend;
            response = new GrpcPrecomputedSyncResponse();
            backend.AddResponseObjectToPrecompute(response);
            termination = new ManualResetEventSlim();
            processThread = new Thread(() =>
            {
                while (!termination.IsSet)
                    backend.Process();
            });
            processThread.Start();
        }

        public void Dispose()
        {
            processThread.Join();
            backend.Dispose();
        }

        public override Task<AddWorkerResponse> AddWorker(AddWorkerRequest request, ServerCallContext context)
        {
            var result = new TaskCompletionSource<AddWorkerResponse>();
            backend.AddWorker(new WorkerId(request.Id),
                r => result.SetResult(new AddWorkerResponse
                    { Id = request.Id, WorldLine = r.Item1, RecoveredVersion = r.Item2 }));
            return result.Task;
        }

        public override Task<RemoveWorkerResponse> RemoveWorker(RemoveWorkerRequest request, ServerCallContext context)
        {
            var result = new TaskCompletionSource<RemoveWorkerResponse>();
            backend.DeleteWorker(new WorkerId(request.Id),
                () => result.SetResult(new RemoveWorkerResponse { Ok = true }));
            return result.Task;
        }

        public override Task<NewCheckpointResponse> NewCheckpoint(NewCheckpointRequest request,
            ServerCallContext context)
        {
            backend.NewCheckpoint(request.WorldLine, new WorkerVersion(request.Id, request.Version),
                request.Deps.Select(wv => new WorkerVersion(wv.Id, wv.Version)));
            return Task.FromResult(new NewCheckpointResponse
            {
                Ok = true
            });
        }

        public override Task<SyncResponse> Sync(SyncRequest request, ServerCallContext context)
        {
            response.rwLatch.EnterReadLock();
            var result = response.obj.Clone();
            response.rwLatch.ExitReadLock();
            return Task.FromResult(result);
        }

        public override Task<ResendGraphResponse> ResendGraph(ResendGraphRequest request, ServerCallContext context)
        {
            foreach (var n in request.GraphNodes)
            {
                backend.NewCheckpoint(n.WorldLine, new WorkerVersion(n.Id, n.Version),
                    n.Deps.Select(wv => new WorkerVersion(wv.Id, wv.Version)));
            }
            backend.MarkWorkerAccountedFor(new WorkerId(request.GraphNodes.First().Id));
            return Task.FromResult(new ResendGraphResponse
            {
                Ok = true
            });
        }
    }
}