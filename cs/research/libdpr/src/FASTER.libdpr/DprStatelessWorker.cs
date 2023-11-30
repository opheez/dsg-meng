using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// A DprSession is a DPR entity that cannot commit/restore state, but communicates with other DPR entities and may
    /// convey DPR dependencies (e.g., a stateless worker participating in speculative execution).
    /// </summary>
    public class DprStatelessWorker<TMessage>
    {
        private EpochProtectedVersionScheme epvs = new EpochProtectedVersionScheme(new LightEpoch());
        private long version, worldLine;
        private readonly LightDependencySet deps;
        private SUId mySU;
        private IDprFinder finder;
        private Action notifyRollback;

        /// <summary>
        /// Version of the session
        /// </summary>
        public long Version => version;

        /// <summary>
        /// WorldLine of the session
        /// </summary>
        public long WorldLine => worldLine;

        /// <summary>
        /// Create a DPR session working on the supplied worldLine (or 1 by default, in a cluster that has never failed)
        /// </summary>
        /// <param name="startWorldLine"> the worldLine to start at, or 1 by default </param>
        public DprStatelessWorker(SUId mySU, Action notifyRollback, IDprFinder finder, long startWorldLine = 1)
        {
            this.mySU = mySU;
            version = 1;
            worldLine = startWorldLine;
            deps = new LightDependencySet();
            this.finder = finder;
            this.notifyRollback = notifyRollback;
        }

        public void Reset(SUId mySU, Action notifyRollback, IDprFinder finder, long startWorldLine = 1)
        {
            this.mySU = mySU;
            foreach (var wv in deps)
            {
                deps.TryRemove(wv.WorkerId, wv.Version);
            }
            version = 1;
            Utility.MonotonicUpdate(ref worldLine, startWorldLine, out _);
            this.finder = finder;
            this.notifyRollback = notifyRollback;
        }

        public void Refresh()
        {
            finder.RefreshStateless();
            var systemWorldLine = finder.SystemWorldLine();
            if (systemWorldLine > worldLine)
            {
                epvs.TryAdvanceVersionWithCriticalSection((x, y) =>
                    {
                        worldLine = systemWorldLine;
                        notifyRollback();
                    },
                    systemWorldLine);
            }
            // TODO(Tianyu): Figure out a way to periodically prune dependency set?
        }


        /// <summary>
        /// Obtain a DPR header that encodes session dependency for an outgoing message
        /// </summary>
        /// <param name="outputHeaderBytes"> byte array to write header into </param>
        /// <returns> size of the header, or negative of the required size to fit if supplied header is to small </returns>
        public unsafe int Send(Span<byte> outputHeaderBytes)
        {
            try
            {
                epvs.Enter();
                fixed (byte* b = outputHeaderBytes)
                {
                    var bend = b + outputHeaderBytes.Length;
                    ref var dprHeader = ref Unsafe.AsRef<DprMessageHeader>(b);

                    // Populate header with relevant request information
                    if (outputHeaderBytes.Length >= DprMessageHeader.FixedLenSize)
                    {
                        dprHeader.SrcWorkerId = WorkerId.INVALID;
                        dprHeader.worldLine = worldLine;
                        dprHeader.version = version;
                        dprHeader.numClientDeps = 0;
                    }

                    // Populate tracking information into the batch
                    var copyHead = b + dprHeader.ClientDepsOffset;
                    foreach (var wv in deps)
                    {
                        dprHeader.numClientDeps++;
                        // only copy if it fits
                        if (copyHead < bend - sizeof(WorkerVersion))
                            Unsafe.AsRef<WorkerVersion>(copyHead) = wv;
                        copyHead += sizeof(WorkerVersion);
                    }

                    // Invert depends on whether or not we fit
                    return (int)(copyHead <= bend ? copyHead - b : b - copyHead);
                }
            }
            finally
            {
                epvs.Leave();
            }
        }

        /// <summary>
        /// Receive a message with the given header in this session. 
        /// </summary>
        /// <param name="dprMessage"> DPR header of the message to receive </param>
        /// <param name="version"> version of the message </param>
        /// <returns> status of the batch. If status is ROLLBACK, this session must be rolled-back </returns>
        public DprReceiveStatus Receive(Span<byte> headerBytes)
        {
            ref var header =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprMessageHeader>(headerBytes));
            try
            {
                epvs.Enter();

                if (header.worldLine < worldLine)
                    return DprReceiveStatus.DISCARD;
                if (header.worldLine > worldLine)
                {
                    var newWl = header.worldLine;
                    epvs.TryAdvanceVersionWithCriticalSection((x, y) =>
                        {
                            worldLine = newWl;
                            notifyRollback();
                        },
                        header.worldLine);
                }
                if (mySU == SUId.EXTERNAL || header.SrcSU != mySU)
                {
                    return finder.SafeVersion(header.SrcWorkerId) >= header.version
                        ? DprReceiveStatus.OK
                        : DprReceiveStatus.BUFFER;
                }

                // Received a message from another stateless worker
                if (!header.SrcWorkerId.Equals(WorkerId.INVALID))
                    deps.Update(header.SrcWorkerId, header.version);
                else
                {
                    unsafe
                    {
                        fixed (byte* d = header.data)
                        {
                            var depsHead = d + header.ClientDepsOffset;
                            for (var i = 0; i < header.numClientDeps; i++)
                            {
                                ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                                deps.Update(wv.WorkerId, wv.Version);
                                depsHead += sizeof(WorkerVersion);
                            }
                        }
                    }
                }

                // Update versioning information
                Utility.MonotonicUpdate(ref version, header.version, out _);
                return DprReceiveStatus.OK;
            }
            finally
            {
                epvs.Leave();
            }
        }
    }
}