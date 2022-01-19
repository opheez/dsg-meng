using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public interface IResizableList
    {
        public int Count();

        public long Read(int index);

        public void Write(int index, long value);

        public int Push(long value);

        public void Delete(int index);
    }


    public class SingleThreadedResizableList : IResizableList
    {
        private long[] list;
        private int count;

        public SingleThreadedResizableList()
        {
            list = new long[1];
            count = 0;
        }

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

        public void Delete(int index)
        {
            if (index < 0 || index >= count) throw new IndexOutOfRangeException();
            count--;
            Array.Copy(list, index + 1, list, index, list.Length - index - 1);
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
            list = new long[1];
            count = 0;
        }

        public int Count() => count;

        public long Read(int index)
        {
            try
            {
                rwLatch.EnterReadLock();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                return list[index];
            }
            finally
            {
                rwLatch.ExitReadLock();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                rwLatch.EnterReadLock();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                rwLatch.ExitReadLock();
            }
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
            if (result == list.Length)
                Resize();
            while (true)
            {
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

        public void Delete(int index)
        {
            try
            {
                rwLatch.EnterWriteLock();
                count--;
                Array.Copy(list, index + 1, list, index, list.Length - index - 1);
            }
            finally
            {
                rwLatch.ExitWriteLock();
            }
        }
    }

    public class SimpleVersionSchemeResizableList : IResizableList
    {
        private SimpleVersionScheme svs;
        private long[] list;
        private int count;

        public SimpleVersionSchemeResizableList()
        {
            svs = new SimpleVersionScheme();
            list = new long[1];
            count = 0;
        }

        public int Count() => count;

        public long Read(int index)
        {
            try
            {
                svs.Enter();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                return list[index];
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
            list = newList;
        }

        public int Push(long value)
        {
            try
            {
                var v = svs.Enter();
                var result = Interlocked.Increment(ref count) - 1;
                if (result == list.Length)
                {
                    svs.AdvanceVersion((_, _) => Resize(), v + 1);
                    while (svs.Refresh() == v)
                        Thread.Yield();
                }

                while (true)
                {
                    if (result >= list.Length) continue;
                    list[result] = value;
                    return result;
                }
            }
            finally
            {
                svs.Leave();
            }
        }

        public void Delete(int index)
        {
            svs.AdvanceVersion((_, _) =>
            {
                count--;
                Array.Copy(list, index + 1, list, index, list.Length - index - 1);
            });
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
                        NotifyAvailableState();
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
    }

    public class TwoPhaseResizableList : IResizableList
    {
        internal EpochProtectedVersionScheme epvs;
        internal long[] list, newList;
        internal int count;

        public TwoPhaseResizableList()
        {
            epvs = new EpochProtectedVersionScheme();
            list = new long[1];
            newList = null;
            count = 0;
        }

        // TODO(Tianyu): How to ensure this is correct in the face of concurrent pushes?
        public int Count() => count;

        public long Read(int index)
        {
            try
            {
                epvs.Enter();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                return list[index];
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
                switch (state.Phase)
                {
                    case VersionSchemeState.REST:
                        if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                        list[index] = value;
                        break;
                    case ListGrowthStateMachine.COPYING:
                        epvs.Leave();
                        Thread.Yield();
                        epvs.Enter();
                        break;
                    default:
                        throw new NotImplementedException();
                }
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
                // Obtain a globally unique index to push entry onto
                var result = Interlocked.Increment(ref count) - 1;

                // Write the entry into the correct underlying array
                while (true)
                {
                    if (state.Phase == VersionSchemeState.REST && result == list.Length)
                        // Use explicit versioning to prevent multiple list growth resulting from same full list state
                        epvs.ExecuteStateMachine(new ListGrowthStateMachine(this, state.Version + 1));

                    var l = state.Phase == VersionSchemeState.REST ? list : newList;
                    if (result >= l.Length)
                    {
                        state = epvs.Refresh();
                        Thread.Yield();
                        continue;
                    }

                    l[result] = value;
                    break;
                }

                return result;
            }
            finally
            {
                epvs.Leave();
            }
        }

        public void Delete(int index)
        {
            epvs.ExecuteStateMachine(new SimpleVersionSchemeStateMachine((_, _) =>
            {
                count--;
                Array.Copy(list, index + 1, list, index, list.Length - index - 1);
            }, epvs));
        }
    }
}