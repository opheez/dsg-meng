using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    internal enum ListImpl
    {
        SingleThreaded, Latched, Svs, TwoPhase
    }
    
    [TestFixture]
    internal class ResizaleListTests
    {
        private IResizableList GetInstance(ListImpl impl)
        {
            switch (impl)
            {
                case ListImpl.SingleThreaded:
                    return new SingleThreadedResizableList();
                case ListImpl.Latched:
                    return new LatchedResizableList();
                case ListImpl.Svs:
                    return new SimpleVersionSchemeResizableList();
                case ListImpl.TwoPhase:
                    return new TwoPhaseResizableList();
                default:
                    throw new NotImplementedException();
            }
        }

        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void BasicSingleThreadedOperationsTest([Values] ListImpl impl)
        {
            var tested = GetInstance(impl);
            Assert.AreEqual(0, tested.Count());

            for (var i = 0; i < 100; i++)
                tested.Push(i);
            Assert.AreEqual(100, tested.Count());
            
            for (var i = 0; i < 100; i++)
                Assert.AreEqual(i, tested.Read(i));

            for (var i = 0; i < 100; i += 2)
                tested.Write(i, i * 2);
            for (var i = 0; i < 100; i++)
            {
                if (i % 2 == 0)
                    Assert.AreEqual(2 * i, tested.Read(i));
                else 
                    Assert.AreEqual(i, tested.Read(i));
            }
        }
        
        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void MultiThreadedPushTest([Values] ListImpl impl)
        {
            var threadInsertCount = 10000;
            var threadCount = 2 * Environment.ProcessorCount;
            if (impl == ListImpl.SingleThreaded)
                Assert.Ignore("Single threaded implementation not tested in multi-threaded tests");

            var tested = GetInstance(impl);

            var referenceList = new List<long>();
            referenceList.AddRange(Enumerable.Repeat<long>(0, threadInsertCount * threadCount));
            var threads = new List<Thread>();
            for (var i = 0; i < threadCount; i++)
            {
                var id = i;
                var threadWorker = new Thread(() =>
                {
                    var random = new Random();
                    for (var j = 0; j < threadInsertCount; j++)
                    {
                        var val = random.Next();
                        var pos = tested.Push(val);
                        referenceList[pos] = val;
                    }
                });
                threadWorker.Start();
                threads.Add(threadWorker);
            }

            foreach (var t in threads) t.Join();
            
            Assert.AreEqual(threadInsertCount * threadCount, tested.Count());
            for (var i = 0; i < referenceList.Count; i++)
            {
                Assert.AreEqual(referenceList[i], tested.Read(i));
            }
        }

        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void MultiThreadedMixedOpTest([Values] ListImpl impl)
        {
            if (impl == ListImpl.SingleThreaded)
                Assert.Ignore("Single threaded implementation not tested in multi-threaded tests");

            var tested = GetInstance(impl);
            var pushCount = 10000;
            var pushThreadCount = Environment.ProcessorCount;
            var writeThreadCount = Environment.ProcessorCount;
            
            var referenceList = new List<long>();
            referenceList.AddRange(Enumerable.Repeat<long>(0, pushCount * pushThreadCount));

            var threads = new List<Thread>();
            for (var i = 0; i < writeThreadCount; i++)
            {
                var id = i;
                var t = new Thread(() =>
                {
                    var random = new Random();
                    var traverseOrder = new List<int>();
                    for (var j = id; j < referenceList.Count(); j += writeThreadCount)
                        traverseOrder.Add(j);
                    traverseOrder = traverseOrder.OrderBy(a => random.Next()).ToList();
                    foreach (var j in traverseOrder)
                    {
                        while (tested.Count() <= j || tested.Read(j) == default)
                            Thread.Yield();
                        tested.Write(j, tested.Read(j) * 2);
                    }
                });
                t.Start();
                threads.Add(t);
            }
            
            for (var i = 0; i < pushThreadCount; i++)
            {
                var id = i;
                var threadWorker = new Thread(() =>
                {
                    var random = new Random();
                    for (var j = 0; j < pushCount; j++)
                    {
                        var val = random.Next();
                        var pos = tested.Push(val);
                        referenceList[pos] = val;
                    }
                });
                threadWorker.Start();
                threads.Add(threadWorker);
            }            
            
            foreach (var t in threads) t.Join();
            
            Assert.AreEqual(referenceList.Count(), tested.Count());
            for (var i = 0; i < referenceList.Count; i++)
            {
                Assert.AreEqual(2 * referenceList[i], tested.Read(i));
            }
        }
    }
}