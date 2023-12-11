using System.Threading;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class SimpleTests
    {
        private ManualResetEventSlim terminationToken;
        private SimulatedDprFinderService simulatedFinderService = new();

        [SetUp]
        public void SetUp()
        {
            terminationToken = new ManualResetEventSlim();
            // Process add request in the background so we do not block the current thread
            simulatedFinderService.ProcessInBackground(terminationToken);
        }

        [TearDown]
        public void TearDown()
        {
            terminationToken.Set();
        }

        private DprStatefulWorker<TestStateObject> ConstructWorker(long id, long suId,
            bool autoCompleteCheckpoints = true)
        {
            return new DprStatefulWorker<TestStateObject>(new WorkerId(id), new SUId(suId),
                new TestStateObject(new WorkerId(id), autoCompleteCheckpoints),
                new TestDprFinder(simulatedFinderService), 0, 0);
        }

        private void SendMessage(DprStatefulWorker<TestStateObject> from, DprStatefulWorker<TestStateObject> to,
            DprReceiveStatus expected)
        {
            var m = from.StateObject().GenerateMessageToSend();
            from.Send(m.dprHeader);
            var status = to.TryReceive<TestMessage>(m.dprHeader, m, out _);
            Assert.AreEqual(expected, status);
            if (status == DprReceiveStatus.OK)
                to.StateObject().Receive(m);
        }

        private void VerifyCommit(params (DprStatefulWorker<TestStateObject>, long)[] expected)
        {
            // Wait a bit for the DprFinder service to catch up
            simulatedFinderService.NextBackgroundProcessComplete().GetAwaiter().GetResult();
            foreach (var (worker, version) in expected)
            {
                worker.ForceRefresh();
                Assert.AreEqual(version, worker.CommittedVersion());
            }
        }

        [Test]
        public void TestOneMessage()
        {
            var tested0 = ConstructWorker(0, 0);
            var tested1 = ConstructWorker(1, 0);

            tested0.ConnectToCluster();
            tested1.ConnectToCluster();
            Assert.AreEqual(1, tested0.Version());
            Assert.AreEqual(1, tested1.Version());
            Assert.AreEqual(1, tested0.WorldLine());
            Assert.AreEqual(1, tested1.WorldLine());

            SendMessage(tested0, tested1, DprReceiveStatus.OK);

            tested1.ForceCheckpoint();
            Assert.AreEqual(2, tested1.Version());
            Assert.AreEqual(1, tested1.WorldLine());
            // Dependencies have not committed, so nothing should commit
            VerifyCommit((tested1, 0));

            tested0.ForceCheckpoint();
            Assert.AreEqual(2, tested0.Version());
            Assert.AreEqual(1, tested0.WorldLine());

            VerifyCommit((tested0, 1), (tested1, 1));
        }

        [Test]
        public void TestThreeServers()
        {
            var a = ConstructWorker(0, 0, false);
            a.ConnectToCluster();
            var b = ConstructWorker(1, 0, false);
            b.ConnectToCluster();
            var c = ConstructWorker(2, 0, false);
            c.ConnectToCluster();

            // Construct a dependency graph without commiting anything
            SendMessage(a, b, DprReceiveStatus.OK);
            a.ForceCheckpoint();
            b.ForceCheckpoint();
            c.ForceCheckpoint();
            SendMessage(b, a, DprReceiveStatus.OK);
            SendMessage(c, b, DprReceiveStatus.OK);
            SendMessage(a, c, DprReceiveStatus.OK);
            a.ForceCheckpoint();
            b.ForceCheckpoint();
            c.ForceCheckpoint();

            // Nothing should commit
            VerifyCommit((a, 0), (b, 0), (c, 0));

            c.StateObject().CompleteCheckpoint(1);
            // C should commit, but nothing else
            VerifyCommit((a, 0), (b, 0), (c, 1));

            b.StateObject().CompleteCheckpoint(1);
            // B still has outstanding dependencies and therefore nothing would commit
            VerifyCommit((a, 0), (b, 0), (c, 1));

            a.StateObject().CompleteCheckpoint(1);
            // Commits can now happen
            VerifyCommit((a, 1), (b, 1), (c, 1));

            b.StateObject().CompleteCheckpoint(2);
            a.StateObject().CompleteCheckpoint(2);
            // Nothing should commit because C still hasn't committed
            VerifyCommit((a, 1), (b, 1), (c, 1));

            c.StateObject().CompleteCheckpoint(2);
            // Now everything should commit
            VerifyCommit((a, 2), (b, 2), (c, 2));
        }

        [Test]
        public void TestSimpleRecovery()
        {
            var a = ConstructWorker(0, 0, false);
            a.ConnectToCluster();
            var b = ConstructWorker(1, 0, false);
            b.ConnectToCluster();
            var c = ConstructWorker(2, 0, false);
            c.ConnectToCluster();

            // Construct a dependency graph without commiting anything
            SendMessage(a, b, DprReceiveStatus.OK);
            a.ForceCheckpoint();
            var a1State = a.StateObject().stateSerialNum;
            b.ForceCheckpoint();
            var b1State = b.StateObject().stateSerialNum;
            c.ForceCheckpoint();
            var c1State = c.StateObject().stateSerialNum;

            SendMessage(b, a, DprReceiveStatus.OK);
            SendMessage(c, b, DprReceiveStatus.OK);
            SendMessage(a, c, DprReceiveStatus.OK);
            a.ForceCheckpoint();
            var a2State = a.StateObject().stateSerialNum;
            Assert.AreNotEqual(a1State, a2State);
            b.ForceCheckpoint();
            var b2State = b.StateObject().stateSerialNum;
            Assert.AreNotEqual(b1State, b2State);
            c.ForceCheckpoint();
            var c2State = c.StateObject().stateSerialNum;
            Assert.AreNotEqual(c1State, c2State);

            // Nothing should commit
            VerifyCommit((a, 0), (b, 0), (c, 0));

            c.StateObject().CompleteCheckpoint(1);
            b.StateObject().CompleteCheckpoint(1);
            a.StateObject().CompleteCheckpoint(1);
            VerifyCommit((a, 1), (b, 1), (c, 1));

            b.StateObject().CompleteCheckpoint(2);
            a.StateObject().CompleteCheckpoint(2);
            VerifyCommit((a, 1), (b, 1), (c, 1));

            // Simulate a failure by reconnecting
            c.ConnectToCluster();
            simulatedFinderService.NextBackgroundProcessComplete().GetAwaiter().GetResult();
            a.ForceRefresh();
            b.ForceRefresh();
            c.ForceRefresh();
            // Everyone should be back at the commit prefix with a larger worldline
            Assert.AreEqual(a1State, a.StateObject().stateSerialNum);
            Assert.AreEqual(2, a.WorldLine());
            Assert.AreEqual(b1State, b.StateObject().stateSerialNum);
            Assert.AreEqual(2, b.WorldLine());
            Assert.AreEqual(c1State, c.StateObject().stateSerialNum);
            Assert.AreEqual(2, c.WorldLine());
        }
    }
}