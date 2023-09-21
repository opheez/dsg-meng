// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace FASTER.core
{
    /// <summary>
    /// Epoch protection
    /// </summary>
    public unsafe sealed class LightEpoch
    {
        /// <summary>
        /// Default invalid index entry.
        /// </summary>
        private const int kInvalidIndex = 0;

        /// <summary>
        /// Default number of entries in the entries table
        /// </summary>
        public static int TableSize = 128;

        /// <summary>
        /// Default drainlist size
        /// </summary>
        private static int DrainListSize = 16;

        /// <summary>
        /// Thread protection status entries.
        /// </summary>
        private EpochTableEntry[] tableRaw;
        private GCHandle tableHandle;
        private EpochTableEntry* tableAligned;

        private static ThreadTableEntry[] threadIndex;
        private static GCHandle threadIndexHandle;
        private static ThreadTableEntry* threadTableAligned;

        /// <summary>
        /// List of action, epoch pairs containing actions to performed 
        /// when an epoch becomes safe to reclaim. Marked volatile to
        /// ensure latest value is seen by the last suspended thread.
        /// </summary>
        private volatile int drainCount = 0;
        private readonly EpochActionPair[] drainList = new EpochActionPair[DrainListSize];

        /// <summary>
        /// A thread's entry in the epoch table.
        /// </summary>
        [ThreadStatic]
        private static int threadEntryIndex;

        [ThreadStatic]
        static int threadId;

        [ThreadStatic]
        static int threadIdHash;

        /// <summary>
        /// Global current epoch value
        /// </summary>
        public int CurrentEpoch;

        /// <summary>
        /// Cached value of latest epoch that is safe to reclaim
        /// </summary>
        public int SafeToReclaimEpoch;

        /// <summary>
        /// Static constructor to setup shared cache-aligned space
        /// to store per-entry count of instances using that entry
        /// </summary>

        public static void InitializeStatic(int tableSize, int drainListSize)
        {
            TableSize = tableSize;
            DrainListSize = drainListSize;
            // Over-allocate to do cache-line alignment
            threadIndex = new ThreadTableEntry[TableSize + 2];
            threadIndexHandle = GCHandle.Alloc(threadIndex, GCHandleType.Pinned);
            long p = (long)threadIndexHandle.AddrOfPinnedObject();

            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1);
            threadTableAligned = (ThreadTableEntry*)p2;
        }
        

        /// <summary>
        /// Instantiate the epoch table
        /// </summary>
        public LightEpoch()
        {
            // Over-allocate to do cache-line alignment
            tableRaw = new EpochTableEntry[TableSize + 2];
            tableHandle = GCHandle.Alloc(tableRaw, GCHandleType.Pinned);
            long p = (long)tableHandle.AddrOfPinnedObject();

            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1);
            tableAligned = (EpochTableEntry*)p2;

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;

            for (int i = 0; i < DrainListSize; i++)
                drainList[i].epoch = int.MaxValue;
            drainCount = 0;
        }

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
            tableHandle.Free();
            tableAligned = null;
            tableRaw = null;
            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
        }

        /// <summary>
        /// Check whether current epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public bool ThisInstanceProtected()
        {
            return threadEntryIndex != kInvalidIndex && (tableAligned + threadEntryIndex)->threadId == threadId;
        }

        /// <summary>
        /// Check whether any epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public static bool AnyInstanceProtected()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex != entry)
                return threadTableAligned->count > 0;
            return false;
        }

        /// <summary>
        /// Enter the thread into the protected code region
        /// </summary>
        /// <returns>Current epoch</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ProtectAndDrain()
        {
            int entry = threadEntryIndex;

            (*(tableAligned + entry)).threadId = threadId;
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }

            return (*(tableAligned + entry)).localCurrentEpoch;
        }

        /// <summary>
        /// Take care of pending drains after epoch suspend
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SuspendDrain()
        {
            while (drainCount > 0)
            {
                // Barrier ensures we see the latest epoch table entries. Ensures
                // that the last suspended thread drains all pending actions.
                Thread.MemoryBarrier();
                for (int index = 1; index <= TableSize; ++index)
                {
                    int entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                    if (0 != entry_epoch)
                    {
                        return;
                    }
                }
                Resume();
                Release();
            }
        }

        /// <summary>
        /// Check and invoke trigger actions that are ready
        /// </summary>
        /// <param name="nextEpoch">Next epoch</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Drain(int nextEpoch)
        {
            ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (int i = 0; i < DrainListSize; i++)
            {
                var trigger_epoch = drainList[i].epoch;

                if (trigger_epoch <= SafeToReclaimEpoch)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue - 1, trigger_epoch) == trigger_epoch)
                    {
                        var trigger_action = drainList[i].action;
                        drainList[i].action = null;
                        drainList[i].epoch = int.MaxValue;
                        Interlocked.Decrement(ref drainCount);
                        trigger_action();
                        if (drainCount == 0) break;
                    }
                }
            }
        }

        /// <summary>
        /// Thread acquires its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Acquire()
        {
            if (threadEntryIndex == kInvalidIndex || (threadTableAligned + threadEntryIndex)->threadId != threadId)
                threadEntryIndex = ReserveEntryForThread();


            Interlocked.Increment(ref (threadTableAligned + threadEntryIndex)->count);

            // In rare races, someone might have kicked us out from the spot concurrently --- should check and rollback
            // our increment if that happens
            if ((threadTableAligned + threadEntryIndex)->threadId != threadId)
            {
                Interlocked.Decrement(ref (threadTableAligned + threadEntryIndex)->count);
                threadEntryIndex = kInvalidIndex;
                Acquire();
            }
            
            Debug.Assert((*(tableAligned + threadEntryIndex)).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");
        }


        /// <summary>
        /// Thread releases its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Release()
        {
            int entry = threadEntryIndex;

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch != 0,
                "Trying to release unprotected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).threadId = 0;
            
            Interlocked.Decrement(ref (threadTableAligned + threadEntryIndex)->count);
        }

        /// <summary>
        /// Thread suspends its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend()
        {
            Release();
            if (drainCount > 0) SuspendDrain();
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume()
        {
            Acquire();
            ProtectAndDrain();
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume(out int resumeEpoch)
        {
            Acquire();
            resumeEpoch = ProtectAndDrain();
        }

        /// <summary>
        /// Increment global current epoch
        /// </summary>
        /// <returns></returns>
        private int BumpCurrentEpoch()
        {
            int nextEpoch = Interlocked.Add(ref CurrentEpoch, 1);

            if (drainCount > 0)
                Drain(nextEpoch);

            return nextEpoch;
        }

        /// <summary>
        /// Increment current epoch and associate trigger action
        /// with the prior epoch
        /// </summary>
        /// <param name="onDrain">Trigger action</param>
        /// <returns></returns>
        public void BumpCurrentEpoch(Action onDrain)
        {
            int PriorEpoch = BumpCurrentEpoch() - 1;

            int i = 0;
            while (true)
            {
                if (drainList[i].epoch == int.MaxValue)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue - 1, int.MaxValue) == int.MaxValue)
                    {
                        drainList[i].action = onDrain;
                        drainList[i].epoch = PriorEpoch;
                        Interlocked.Increment(ref drainCount);
                        break;
                    }
                }
                else
                {
                    var triggerEpoch = drainList[i].epoch;

                    if (triggerEpoch <= SafeToReclaimEpoch)
                    {
                        if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue - 1, triggerEpoch) == triggerEpoch)
                        {
                            var triggerAction = drainList[i].action;
                            drainList[i].action = onDrain;
                            drainList[i].epoch = PriorEpoch;
                            triggerAction();
                            break;
                        }
                    }
                }

                if (++i == DrainListSize)
                {
                    ProtectAndDrain();
                    i = 0;
                    Thread.Yield();
                }
            }

            ProtectAndDrain();
        }

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <param name="currentEpoch">Current epoch</param>
        /// <returns>Safe epoch</returns>
        private int ComputeNewSafeToReclaimEpoch(int currentEpoch)
        {
            int oldestOngoingCall = currentEpoch;

            for (int index = 1; index <= TableSize; ++index)
            {
                int entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch)
                {
                    if (entry_epoch < oldestOngoingCall)
                    {
                        oldestOngoingCall = entry_epoch;
                    }
                }
            }

            // The latest safe epoch is the one just before 
            // the earliest unsafe epoch.
            SafeToReclaimEpoch = oldestOngoingCall - 1;
            return SafeToReclaimEpoch;
        }

        /// <summary>
        /// Reserve entry for thread. This method relies on the fact that no
        /// thread will ever have ID 0.
        /// </summary>
        /// <param name="startIndex">Start index</param>
        /// <param name="threadId">Thread id</param>
        /// <returns>Reserved entry</returns>
        private static int ReserveEntry(int startIndex)
        {
            int current_iteration = 0;
            for (; ; )
            {
                // Reserve an entry in the table.
                for (int i = 0; i < TableSize; ++i)
                {
                    int index_to_test = 1 + ((startIndex + i) & (TableSize - 1));
                    var entry = threadTableAligned + index_to_test;
                    var word = entry->word;
                    var wordThreadId = word >> 32;
                    var wordCount = word & 0xFFFFFFFF;
                    if (wordThreadId == 0 || wordCount == 0)
                    {
                        bool success =
                            (word == Interlocked.CompareExchange(
                                ref entry->word, ((long) threadId) << 32, word));
            
                        if (success)
                        {
                            return (int)index_to_test;
                        }
                    }
                    ++current_iteration;
                }
            
                if (current_iteration > (TableSize * 20))
                {
                    throw new FasterException("Unable to reserve an epoch entry, try increasing the epoch table size (kTableSize)");
                }
            }
        }

        /// <summary>
        /// Allocate a new entry in epoch table. This is called 
        /// once for a thread.
        /// </summary>
        /// <returns>Reserved entry</returns>
        private static int ReserveEntryForThread()
        {
            if (threadId == 0) // run once per thread for performance
            {
                // For portability (run on non-windows platform)
                threadId = Environment.OSVersion.Platform == PlatformID.Win32NT ? (int)Native32.GetCurrentThreadId() : Thread.CurrentThread.ManagedThreadId;
                threadIdHash = Utility.Murmur3(threadId);
            }
            return ReserveEntry(threadIdHash);
        }

        /// <summary>
        /// Epoch table entry (cache line size).
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Constants.kCacheLineBytes)]
        private struct EpochTableEntry
        {
            /// <summary>
            /// Thread-local value of epoch
            /// </summary>
            [FieldOffset(0)]
            public int localCurrentEpoch;

            [FieldOffset(4)] public long word;
            
            /// <summary>
            /// ID of thread associated with this entry.
            /// </summary>
            [FieldOffset(4)]
            public int threadId;

            [FieldOffset(8)]
            public int reentrant;

            [FieldOffset(12)]
            public fixed int markers[13];
        };
        
        /// <summary>
        /// Epoch table entry (cache line size).
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Constants.kCacheLineBytes)]
        private struct ThreadTableEntry
        {
            [FieldOffset(0)] public long word;
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int ThreadIdFromWord(long word) => (int) (word >> 32);
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int CountFromWord(long word) => (int) (word & 0xFFFFFFFF);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static long FormWord(int threadId, int count) => ((long)threadId << 32) | ((long)count);

            /// <summary>
            /// ID of thread associated with this entry.
            /// </summary>
            [FieldOffset(4)]
            public int threadId;

            [FieldOffset(8)]
            public int count;
        };

        private struct EpochActionPair
        {
            public long epoch;
            public Action action;
        }

        /// <summary>
        /// Mechanism for threads to mark some activity as completed until
        /// some version by this thread
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Mark(int markerIdx, long version)
        {
            (*(tableAligned + threadEntryIndex)).markers[markerIdx] = (int)version;
        }

        /// <summary>
        /// Check if all active threads have completed the some
        /// activity until given version.
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns>Whether complete</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CheckIsComplete(int markerIdx, long version)
        {
            // check if all threads have reported complete
            for (int index = 1; index <= TableSize; ++index)
            {
                int entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                int fc_version = (*(tableAligned + index)).markers[markerIdx];
                if (0 != entry_epoch)
                {
                    if ((fc_version != (int)version) && (entry_epoch < int.MaxValue))
                    {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}