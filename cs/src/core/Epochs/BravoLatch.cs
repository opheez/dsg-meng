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
        
        public bool EnterReadLock()
        {
            if (rBias)
            {
                var result = Interlocked.CompareExchange(ref visibleReadersTable.GetSlotForCurrentThread(), this, null);
                if (result == null)
                {
                    if (rBias) return false;
                    visibleReadersTable.GetSlotForCurrentThread() = null;
                }

                // if (result != null && result != this)
                //     Console.WriteLine("Collision across instance");
                // else if (rBias)
                //     Console.WriteLine("Collision within instance");
            }
            
            // Slowpath
            underlying.EnterReadLock();
            if (!rBias && DateTime.Now > inhibitUntil)
                rBias = true;
            return true;
        }

        public void ExitReadLock(bool token)
        {
            if (token)
            {
                underlying.ExitReadLock();
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