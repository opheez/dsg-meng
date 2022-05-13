using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// StateObject that encapsulates a single atomic counter
    /// </summary>
    /// A counter is an example of a simple state object --- one that does not need the fine grained version control
    /// ability exposed by the full StateObject interface for concurrent access performance or (single-node) checkpoint
    /// coordination. We can therefore just extend from SimpleStateObject
    public sealed class CounterStateObject : SimpleStateObject
    {
        private string checkpointDirectory;
        private ConcurrentDictionary<long, long> prevCounters = new();
        public long value;

        /// <summary>
        /// Constructs a new CounterStateObject
        /// </summary>
        /// <param name="checkpointDirectory">directory name to write checkpoints to</param>
        /// <param name="version">
        /// version to start at. If version is not 0, CounterStateObject will attempt to restore
        /// state from corresponding checkpoint
        /// </param>
        public CounterStateObject(string checkpointDirectory)
        {
            this.checkpointDirectory = checkpointDirectory;
            Directory.CreateDirectory(checkpointDirectory);
        }
        
        // With SimpleStateObject, CounterStateObject only needs to implement a single-threaded
        // checkpoint scheme.
        protected override void PerformCheckpoint(long version, ReadOnlySpan<byte> deps, Action onPersist)
        {
            // Use a simple naming scheme to associate checkpoints with versions. A more sophisticated scheme may
            // store persistent mappings or use other schemes to do so.
            var fileName = Path.Join(checkpointDirectory, version.ToString());
            var fs = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

            // libDPR will ensure that request batches that are protected with VersionScheme.Enter() and
            // VersionScheme.Leave() will not interleave with checkpoint or recovery code. It is therefore safe
            // to read and write values without protection in this function
            prevCounters[version] = value;
            
            // Once the content of the checkpoint is established (we have read a current snapshot of value), it is ok
            // to write to disk asynchronously and allow other operations to continue. In SimpleStateObject, 
            // operations are blocked before PerformCheckpoint return.
            // var serializationBuffer = new byte[sizeof(long) + sizeof(int) + deps.Length];
            var serializationBuffer = new byte[deps.Length];
            unsafe {
                fixed (byte* s = serializationBuffer) {
                    // *(long*) s = value;
                    // *(int*) (s + sizeof(long)) = deps.Length;
                    deps.CopyTo(new Span<byte>(s, deps.Length));
                    // deps.CopyTo(new Span<byte>(s + sizeof(long) + sizeof(int), deps.Length));
                }
            } 
            // fs.WriteAsync(BitConverter.GetBytes(value), 0, sizeof(long)).ContinueWith(token =>
            // {
            //     if (!token.IsCompletedSuccessfully)
            //         Console.WriteLine($"Error {token} during checkpoint");
            //     // We need to invoke onPersist() to inform DPR when a checkpoint is on disk
            //     onPersist();
            //     fs.Dispose();
            // });
            fs.WriteAsync(serializationBuffer).AsTask().ContinueWith(token =>
            {
                if (!token.IsCompletedSuccessfully)
                    Console.WriteLine($"Error {token} during checkpoint");
                // We need to invoke onPersist() to inform DPR when a checkpoint is on disk
                onPersist();
                fs.Dispose();
            });
        }
        
        // With SimpleStateObject, CounterStateObject can just implement a single-threaded blocking recovery function
        protected override void RestoreCheckpoint(long version)
        {   
            // This is for machines that did not physically go down (otherwise they will simply
            // load the surviving version on restart). libDPR will additionally never request a worker to restore
            // checkpoints earlier than the committed version in the DPR cut. We can therefore rely on a (relatively
            // small) stash of in-memory snapshots to quickly handle this call.
            if (prevCounters.TryGetValue(version, out value)) return;
            
            var fileName = Path.Join(checkpointDirectory, version.ToString());
            using var fs = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.None);

            var bytes = new byte[sizeof(long)];
            fs.Read(bytes, 0, sizeof(long));
            value = BitConverter.ToInt64(bytes, 0);
        }
        
        public override void PruneVersion(long version)
        {
            lock(prevCounters)
            {
                var fileToDelete = Path.Join(checkpointDirectory, version.ToString());
                prevCounters.TryRemove(version, out _);
                File.Delete(fileToDelete);
            }
            // TODO: Remove the file with the version
        }

        public override IEnumerable<(byte[], int)> GetUnprunedVersions()
        {
            lock(prevCounters) // TODO(Nikola): This fixes the below problem (I think), figure out if the scope can be made tighter
            {
                (byte[], int)[] unpruned = new (byte[], int)[prevCounters.Count()];
                int index = 0;
                foreach(var (version, _) in prevCounters)
                {
                    var fileToOpen = Path.Join(checkpointDirectory, version.ToString());
                    var fileBytes = File.ReadAllBytes(fileToOpen);
                    unpruned[index] = (fileBytes, 0);
                    index++;
                }
                return unpruned;
            }
            // return Enumerable.Empty<(byte[], int)>();
            // TODO: pairs of byte arrays (deps) from each file with the dep byte starting at zero, size of array
        }
    }
}