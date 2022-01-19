using System;
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
                    return new SingleThreadedResizableList();
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

            tested.Delete(0);
            Assert.AreEqual(99, tested.Count());
            for (var i = 0; i < 99; i++)
            {
                if (i % 2 == 1)
                    Assert.AreEqual((i + 1) * 2, tested.Read(i));
                else 
                    Assert.AreEqual(i + 1, tested.Read(i));
            }
        }
        
    }
}