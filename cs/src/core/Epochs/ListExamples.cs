using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public interface IResizableList
    {
        public void InitializeThread();

        public void TeardownThread();
        
        public int Count();

        public long Read(int index);

        public void Write(int index, long value);

        public int Push(long value);
    }


    public class SingleThreadedResizableList : IResizableList
    {
        private long[] list;
        private int count;

        public SingleThreadedResizableList()
        {
            list = new long[16];
            count = 0;
        }

        public void InitializeThread() {}

        public void TeardownThread() {}
        

        public int Count() => count;

        public long Read(int index)
        {
            if (index < 0 || index >= count) throw new IndexOutOfRangeException();
            return list[index];
        }

        public void Write(int index, long value)
        {
            if (index < 0 || index >= count) throw new IndexOutOfRangeException();
            list[index] = value;
        }

        public int Push(long value)
        {
            if (count == list.Length)
            {
                var newList = new long[2 * count];
                Array.Copy(list, newList, list.Length);
                list = newList;
            }

            list[count] = value;
            return count++;
        }
    }

    public class MockLatchFreeResizableList : IResizableList
    {
        private long[] list;
        private int count;

        public MockLatchFreeResizableList(long maxCapacity)
        {
            list = new long[maxCapacity];
            count = 0;
        }
        
        public void InitializeThread() {}

        public void TeardownThread() {}

        public int Count() => count;

        public long Read(int index)
        {
            return list[index];
        }

        public void Write(int index, long value)
        {
            list[index] = value;
        }

        public int Push(long value)
        {
            var result = Interlocked.Increment(ref count) - 1;
            if (result == list.Length) throw new FasterException("list full! Increase mock list size");
            list[result] = value;
            return result;
        }
    }

    public class BravoResizableList : IResizableList
    {
        private BravoLatch rwLatch = new();
        private long[] list = new long[16];
        private int count = 0;

        public void InitializeThread() {}

        public void TeardownThread() {}
        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            var token = rwLatch.EnterReadLock();
            try
            {
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                rwLatch.ExitReadLock(token);
            }
        }

        public void Write(int index, long value)
        {
            var token = rwLatch.EnterReadLock();
            try
            {
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                rwLatch.ExitReadLock(token);
            }
        }

        private void Resize()
        {
            try
            {
                rwLatch.EnterWriteLock();
                var newList = new long[2 * list.Length];
                Array.Copy(list, newList, list.Length);
                // Thread.Sleep(10);
                list = newList;
            }
            finally
            {
                rwLatch.ExitWriteLock();
            }
        }

        public int Push(long value)
        {
            var result = Interlocked.Increment(ref count) - 1;
            while (true)
            {
                if (result == list.Length)
                    Resize();
                var token = rwLatch.EnterReadLock();

                try
                {
                    if (result >= list.Length) continue;
                    list[result] = value;
                    return result;
                }
                finally
                {
                    rwLatch.ExitReadLock(token);
                }
            }
        }
    }

    public class LatchedResizableList : IResizableList
    {
        private ReaderWriterLockSlim rwLatch;
        private long[] list;
        private int count;

        public LatchedResizableList()
        {
            rwLatch = new ReaderWriterLockSlim();
            list = new long[16];
            count = 0;
        }
        
        public void InitializeThread() {}

        public void TeardownThread() {}

        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                rwLatch.EnterReadLock();
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                rwLatch.ExitReadLock();
            }
        }

        public void Write(int index, long value)
        {
            rwLatch.EnterReadLock();
            list[index] = value;
            rwLatch.ExitReadLock();
        }

        private void Resize()
        {
            try
            {
                rwLatch.EnterWriteLock();
                var newList = new long[2 * list.Length];
                Array.Copy(list, newList, list.Length);
                list = newList;
            }
            finally
            {
                rwLatch.ExitWriteLock();
            }
        }

        public int Push(long value)
        {
            var result = Interlocked.Increment(ref count) - 1;
            while (true)
            {
                if (result == list.Length)
                    Resize();
                try
                {
                    rwLatch.EnterReadLock();
                    if (result >= list.Length) continue;
                    list[result] = value;
                    return result;
                }
                finally
                {
                    rwLatch.ExitReadLock();
                }
            }
        }
    }

    public class PinnedEpvsResizableList : IResizableList
    {
        private EpochProtectedVersionScheme epvs;
        private long[] list;
        private int count;

        public PinnedEpvsResizableList()
        {
            epvs = new EpochProtectedVersionScheme(new LightEpoch());
            list = new long[16];
            count = 0;
        }

        public void InitializeThread() => epvs.Enter();

        public void TeardownThread() => epvs.Leave();

        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                epvs.Refresh();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                epvs.Refresh();
            }
        }

        private void Resize()
        {
            var newList = new long[2 * list.Length];
            Array.Copy(list, newList, list.Length);
            // Thread.Sleep(10);
            list = newList;
        }

        public int Push(long value)
        {
            try
            {
                var v = epvs.CurrentState();
                var result = Interlocked.Increment(ref count) - 1;

                while (true)
                {
                    if (result < list.Length)
                    {
                        list[result] = value;
                        return result;
                    }

                    epvs.Leave();
                    if (result == list.Length)
                        epvs.AdvanceVersion((_, _) => Resize(), v.Version + 1);
                    Thread.Yield();
                    v = epvs.Enter();
                }
            }
            finally
            {
                epvs.Refresh();
            }
        }
    }

    public class SimpleVersionSchemeResizableList : IResizableList
    {
        private EpochProtectedVersionScheme svs;
        private long[] list;
        private int count;

        public SimpleVersionSchemeResizableList()
        {
            svs = new EpochProtectedVersionScheme(new LightEpoch());
            list = new long[16];
            count = 0;
        }
        
        public void InitializeThread() {}

        public void TeardownThread() {}

        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                svs.Enter();
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                svs.Leave();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                svs.Enter();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                svs.Leave();
            }
        }

        private void Resize()
        {
            var newList = new long[2 * list.Length];
            Array.Copy(list, newList, list.Length);
            // Thread.Sleep(10);
            list = newList;
        }

        public int Push(long value)
        {
            try
            {
                var v = svs.Enter();
                var result = Interlocked.Increment(ref count) - 1;

                while (true)
                {
                    if (result < list.Length)
                    {
                        list[result] = value;
                        return result;
                    }

                    svs.Leave();
                    if (result == list.Length)
                        svs.AdvanceVersion((_, _) => Resize(), v.Version + 1);
                    Thread.Yield();
                    v = svs.Enter();
                }
            }
            finally
            {
                svs.Leave();
            }
        }
    }

    public class ListGrowthStateMachine : VersionSchemeStateMachine
    {
        public const byte COPYING = 1;
        private TwoPhaseResizableList obj;
        private volatile bool copyDone = false;

        public ListGrowthStateMachine(TwoPhaseResizableList obj, long toVersion) : base(obj.epvs, toVersion)
        {
            this.obj = obj;
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            switch (currentState.Phase)
            {
                case VersionSchemeState.REST:
                    nextState = VersionSchemeState.Make(COPYING, currentState.Version);
                    return true;
                case COPYING:
                    nextState = VersionSchemeState.Make(VersionSchemeState.REST, actualToVersion);
                    return copyDone;
                default:
                    throw new NotImplementedException();
            }
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            switch (fromState.Phase)
            {
                case VersionSchemeState.REST:
                    obj.newList = new long[obj.list.Length * 2];
                    Task.Run(() =>
                    {
                        Array.Copy(obj.list, obj.newList, obj.list.Length);
                        copyDone = true;
                        // Thread.Sleep(10);
                        Thread.Yield();
                        epvs.TryStepStateMachine();
                    });
                    break;
                case COPYING:
                    obj.list = obj.newList;
                    obj.newList = null;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        public override void AfterEnteringState(VersionSchemeState state)
        {
            // if (state.Phase == COPYING)
            // {
            //     Array.Copy(obj.list, obj.newList, obj.list.Length);
            //     copyDone = true;
            //     epvs.TryStepStateMachine();
            // }
        }  
    }

    public class TwoPhaseResizableList : IResizableList
    {
        internal EpochProtectedVersionScheme epvs;
        internal long[] list, newList;
        internal int count;

        public TwoPhaseResizableList()
        {
            epvs = new EpochProtectedVersionScheme();
            list = new long[16];
            newList = null;
            count = 0;
        }
        
        public void InitializeThread() {}

        public void TeardownThread() {}

        // TODO(Tianyu): How to ensure this is correct in the face of concurrent pushes?
        public int Count() => count;

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                var state = epvs.Enter();
                switch (state.Phase)
                {
                    case VersionSchemeState.REST:
                        if (index < list.Length) return list[index];
                        // element allocated but yet to be constructed
                        if (index < count) return default;
                        throw new IndexOutOfRangeException();
                    case ListGrowthStateMachine.COPYING:
                        if (index < list.Length) return list[index];
                        if (index < newList.Length) return newList[index];
                        if (index < count) return default;
                        throw new IndexOutOfRangeException();
                    default:
                        throw new NotImplementedException();
                }
            }
            finally
            {
                epvs.Leave();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                var state = epvs.Enter();
                if (index < 0 || index >= count)
                    throw new IndexOutOfRangeException();
                // Write operation is not allowed during copy because we don't know about the copy progress
                while (state.Phase == ListGrowthStateMachine.COPYING || index >= list.Length)
                    state = epvs.Refresh();
                list[index] = value;
            }
            finally
            {
                epvs.Leave();
            }
        }

        public int Push(long value)
        {
            try
            {
                var state = epvs.Enter();
                var result = Interlocked.Increment(ref count) - 1;
                // Write the entry into the correct underlying array
                while (true)
                {
                    switch (state.Phase)
                    {
                        case VersionSchemeState.REST:
                            if (result >= list.Length)
                            {
                                epvs.Leave();
                                if (result == list.Length)
                                    epvs.ExecuteStateMachine(new ListGrowthStateMachine(this, state.Version + 1));
                                Thread.Yield();
                                state = epvs.Enter();
                                continue;
                            }

                            list[result] = value;
                            return result;
                        case ListGrowthStateMachine.COPYING:
                            // This was the copying phase of a previous state machine
                            if (result >= newList.Length)
                            {
                                epvs.Leave();
                                Thread.Yield();
                                state = epvs.Enter();
                                continue;
                            }

                            // Make sure to write update to old list if it belongs there in case copying erases new write
                            if (result < list.Length)
                                list[result] = value;
                            // Also write to new list in case copying was delayed
                            newList[result] = value;
                            return result;
                    }
                }
            }
            finally
            {
                epvs.Leave();
            }
        }
    }
}