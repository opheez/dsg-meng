using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    ///     Class used to inexpensively track worker version dependency for a workers and client sessions.
    ///     Can only correctly track dependencies within a cluster up to size MaxClusterSize
    /// </summary>
    public sealed class LightDependencySet : IEnumerable<WorkerVersion>, IDisposable
    {
        private const int MaxSizeBits = 8;

        /**
         * The maximum number of workers in a cluster this light dependency set can support. Compile-time constant.
         */
        public const int MaxClusterSize = 1 << MaxSizeBits;

        private const long NoDependency = -1;
        private readonly long[] dependentVersions;

        /// <summary>
        ///     Constructs a new light dependency set
        /// </summary>
        public LightDependencySet()
        {
            dependentVersions = new long[1 << MaxSizeBits];
            for (var i = 0; i < dependentVersions.Length; i++)
                dependentVersions[i] = NoDependency;
        }
        
        public void Dispose() {}

        /// <inheritdoc />
        public IEnumerator<WorkerVersion> GetEnumerator()
        {
            return new LightDependencySetEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        ///     Add dependency of (worker, version)
        /// </summary>
        /// <param name="dprWorkerId">worker</param>
        /// <param name="version">version</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(DprWorkerId dprWorkerId, long version)
        {
            ref var originalVersion = ref dependentVersions[dprWorkerId.guid];
            core.Utility.MonotonicUpdate(ref originalVersion, version, out _);
        }

        /// <summary>
        ///     Removes the dependency of (worker, version) and all previous versions of the worker if present
        /// </summary>
        /// <param name="dprWorkerId"> worker </param>
        /// <param name="version"> version </param>
        /// <returns>whether the dependency was removed</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRemove(DprWorkerId dprWorkerId, long version)
        {
            ref var originalVersion = ref dependentVersions[dprWorkerId.guid];
            return Interlocked.CompareExchange(ref originalVersion, NoDependency, version) >= version;
        }

        private class LightDependencySetEnumerator : IEnumerator<WorkerVersion>
        {
            private readonly LightDependencySet dependencySet;
            private int index = -1;

            public LightDependencySetEnumerator(LightDependencySet dependencySet)
            {
                this.dependencySet = dependencySet;
            }

            public bool MoveNext()
            {
                while (++index < MaxClusterSize)
                    if (dependencySet.dependentVersions[index] != NoDependency)
                        return true;
                return false;
            }

            public void Reset()
            {
                index = -1;
            }

            public WorkerVersion Current => new WorkerVersion(index, dependencySet.dependentVersions[index]);

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
    }
}