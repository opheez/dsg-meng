using System;
using System.Diagnostics;
using System.Threading;

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
                if (count != list.Length) return;
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
            while (true)
            {
                Debug.Assert(count <= list.Length);
                var countLocal = count;
                if (countLocal == list.Length)
                {
                    Resize();
                    continue;
                }

                try
                {
                    rwLatch.EnterReadLock();
                    var result = countLocal + 1;
                    if (Interlocked.CompareExchange(ref count, countLocal, result) == countLocal)
                    {
                        list[result] = value;
                        return result;
                    }
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
                while (true)
                {
                    Debug.Assert(count <= list.Length);
                    var countLocal = count;
                    if (countLocal == list.Length)
                    {
                        svs.AdvanceVersion((_, _) => Resize(), v + 1);
                        while (svs.Refresh() == v)
                            Thread.Yield();
                    }

                    var result = countLocal + 1;
                    if (Interlocked.CompareExchange(ref count, countLocal, result) == countLocal)
                    {
                        list[result] = value;
                        return result;
                    }
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
}