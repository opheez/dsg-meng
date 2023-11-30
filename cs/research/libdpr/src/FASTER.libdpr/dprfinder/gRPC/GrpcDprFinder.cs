using System;
using System.Collections.Generic;
using FASTER.libdpr.proto;
using Grpc.Net.Client;

namespace FASTER.libdpr
{
    public class GrpcDprFinder : DprFinderBase
    {
        private DprFinder.DprFinderClient finderClient;

        public GrpcDprFinder(GrpcChannel channel)
        {
            finderClient = new DprFinder.DprFinderClient(channel);
        }

        public override void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted,
            IEnumerable<WorkerVersion> deps)
        {
            var request = new NewCheckpointRequest
            {
                Id = persisted.WorkerId.guid,
                Version = persisted.Version,
                WorldLine = worldLine
            };
            foreach (var dep in deps)
                request.Deps.Add(new proto.WorkerVersion
                {
                    Id = dep.WorkerId.guid,
                    Version = dep.Version
                });

            // Can just leave async without waiting to complete
            finderClient.NewCheckpointAsync(request);
        }

        protected override bool Sync(ClusterState stateToUpdate, Dictionary<WorkerId, long> cutToUpdate)
        {
            var response = finderClient.Sync(new SyncRequest());
            if (response.CurrentCut.Count == 0) return false;
            
            stateToUpdate.currentWorldLine = response.WorldLine;
            foreach (var entry in response.WorldLinePrefix)
                stateToUpdate.worldLinePrefix.Add(new WorkerId(entry.Id), entry.Version);
            foreach (var entry in response.CurrentCut)
                cutToUpdate.Add(new WorkerId(entry.Id), entry.Version);
            return true;
        }

        protected override void SendGraphReconstruction(WorkerId id, IStateObject stateObject)
        {
            var checkpoints = stateObject.GetUnprunedVersions();
            var request = new ResendGraphRequest();
            foreach (var (bytes, offset) in checkpoints)
            {
                SerializationUtil.DeserializeCheckpointMetadata(new Span<byte>(bytes, offset, bytes.Length - offset),
                    out var worldLine, out var wv, out var deps);
                var checkpointRequest = new NewCheckpointRequest
                {
                    Id = id.guid,
                    Version = wv.Version,
                    WorldLine = worldLine
                };
                foreach (var dep in deps)
                    checkpointRequest.Deps.Add(new proto.WorkerVersion
                    {
                        Id = dep.WorkerId.guid,
                        Version = dep.Version
                    });
                request.GraphNodes.Add(checkpointRequest);
                finderClient.NewCheckpoint(checkpointRequest);
            }

            finderClient.ResendGraph(request);        
        }

        protected override void AddWorkerInternal(WorkerId id)
        {
            finderClient.AddWorker(new AddWorkerRequest
            {
                Id = id.guid
            });
        }

        public override void RemoveWorker(WorkerId id)
        {
            finderClient.RemoveWorker(new RemoveWorkerRequest
            {
                Id = id.guid
            });
        }
    }
}