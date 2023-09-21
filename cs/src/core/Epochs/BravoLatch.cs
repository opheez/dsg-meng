using System;
using System.Data;
using System.Threading;

namespace FASTER.core
{
    public class VisibleReadersTable
    {
        public static int TableSize = 4096;

        private BravoLatch[] visibleReaders = new BravoLatch[TableSize];

        [ThreadStatic] static int threadId;
        [ThreadStatic] static int threadIdHash;

        public ref BravoLatch GetSlotForCurrentThread()
        {
            if (threadId == 0) // run once per thread for performance
            {
                // For portability (run on non-windows platform)
                threadId = Environment.OSVersion.Platform == PlatformID.Win32NT
                    ? (int)Native32.GetCurrentThreadId()
                    : Thread.CurrentThread.ManagedThreadId;
                threadIdHash = Utility.Murmur3(threadId) & (TableSize - 1);
            }

            return ref visibleReaders[threadIdHash];
        }

        public void WaitUntilReaderEmpty(BravoLatch latch)
        {
            for (var i = 0; i < TableSize; i++)
                while (visibleReaders[i] == latch)
                    Thread.Yield();
        }
    }

    public unsafe class BravoLatch
    {
        private bool rBias = true;
        private DateTime inhibitUntil = DateTime.MinValue;
        private ReaderWriterLockSlim underlying = new();

        private static VisibleReadersTable visibleReadersTable = new();
        [ThreadStatic] static bool readerLockHeld;
        
        public void EnterReadLock()
        {
            if (rBias)
            {
                if (Interlocked.CompareExchange(ref visibleReadersTable.GetSlotForCurrentThread(), this, null) == null)
                {
                    if (rBias) return;
                    visibleReadersTable.GetSlotForCurrentThread() = null;
                }
            }

            // Slowpath
            underlying.EnterReadLock();
            readerLockHeld = true;
            if (!rBias && DateTime.Now > inhibitUntil)
                rBias = true;
        }

        public void ExitReadLock()
        {
            if (readerLockHeld)
            {
                underlying.ExitReadLock();
                readerLockHeld = false;
            }
            else
            {
                visibleReadersTable.GetSlotForCurrentThread() = null;
            }
        }

        public void EnterWriteLock()
        {
            underlying.EnterWriteLock();
            if (rBias)
            {
                rBias = false;
                var start = DateTime.Now;
                visibleReadersTable.WaitUntilReaderEmpty(this);
                var elapsedTime = DateTime.Now - start;
                inhibitUntil = start + TimeSpan.FromTicks(9 * elapsedTime.Ticks);
            }
        }

        public void ExitWriteLock()
        {
            underlying.ExitWriteLock();
        }
    }
}