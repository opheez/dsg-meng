using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.libdpr.proto;
using Grpc.Core;
using Microsoft.Extensions.Hosting;

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
            rwLatch.ExitWriteLock();
        }

        public override void UpdateCut(Dictionary<DprWorkerId, long> newCut)
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
    
    public class DprFinderGrpcBackgroundService : BackgroundService 
    {
        private readonly GraphDprFinderBackend backend;
        private GrpcPrecomputedSyncResponse response;
        
        public DprFinderGrpcBackgroundService(GraphDprFinderBackend backend)
        {
            this.backend = backend;
            response = new GrpcPrecomputedSyncResponse();
            backend.AddResponseObjectToPrecompute(response);
        }
        
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                backend.Process();
                await Task.Yield();
            }
        }

        public Task<AddWorkerResponse> AddWorker(AddWorkerRequest request)
        {
            var result = new TaskCompletionSource<AddWorkerResponse>();
            backend.AddWorker(new DprWorkerId(request.Id),
                r => result.SetResult(new AddWorkerResponse
                    { Id = request.Id, WorldLine = r.Item1, RecoveredVersion = r.Item2 }));
            return result.Task;
        }

        public Task<RemoveWorkerResponse> RemoveWorker(RemoveWorkerRequest request)
        {
            var result = new TaskCompletionSource<RemoveWorkerResponse>();
            backend.DeleteWorker(new DprWorkerId(request.Id),
                () => result.SetResult(new RemoveWorkerResponse { Ok = true }));
            return result.Task;
        }

        public Task<NewCheckpointResponse> NewCheckpoint(NewCheckpointRequest request)
        {
            backend.NewCheckpoint(request.WorldLine, new WorkerVersion(request.Id, request.Version),
                request.Deps.Select(wv => new WorkerVersion(wv.Id, wv.Version)));
            return Task.FromResult(new NewCheckpointResponse
            {
                Ok = true
            });
        }

        public Task<SyncResponse> Sync()
        {
            response.rwLatch.EnterReadLock();
            var result = response.obj.Clone();
            response.rwLatch.ExitReadLock();
            return Task.FromResult(result);
        }

        public Task<ResendGraphResponse> ResendGraph(ResendGraphRequest request)
        {
            foreach (var n in request.GraphNodes)
            {
                backend.NewCheckpoint(n.WorldLine, new WorkerVersion(n.Id, n.Version),
                    n.Deps.Select(wv => new WorkerVersion(wv.Id, wv.Version)));
            }
            backend.MarkWorkerAccountedFor(new DprWorkerId(request.Id));
            return Task.FromResult(new ResendGraphResponse
            {
                Ok = true
            });
        }
    }
    
    public class DprFinderGrpcService : DprFinder.DprFinderBase
    {
        private DprFinderGrpcBackgroundService backend;
        
        public DprFinderGrpcService(DprFinderGrpcBackgroundService backend)
        {
            this.backend = backend;
        }

        public override Task<AddWorkerResponse> AddWorker(AddWorkerRequest request, ServerCallContext context)
        {
            return backend.AddWorker(request);
        }

        public override Task<RemoveWorkerResponse> RemoveWorker(RemoveWorkerRequest request, ServerCallContext context)
        {
            return backend.RemoveWorker(request);
        }

        public override Task<NewCheckpointResponse> NewCheckpoint(NewCheckpointRequest request,
            ServerCallContext context)
        {
            return backend.NewCheckpoint(request);
        }

        public override Task<SyncResponse> Sync(SyncRequest request, ServerCallContext context)
        {
            return backend.Sync();
        }

        public override Task<ResendGraphResponse> ResendGraph(ResendGraphRequest request, ServerCallContext context)
        {
            return backend.ResendGraph(request);
        }
    }
}