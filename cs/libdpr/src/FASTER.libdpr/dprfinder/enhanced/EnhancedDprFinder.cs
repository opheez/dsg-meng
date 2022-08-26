using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.libdpr
{
    public class EnhancedDprFinder : IDprFinder
    {
        private static readonly string basicLog = "/DprCounters/data/basic.txt";
        private Socket dprFinderConn;
        private string ip;
        private int port;
        private ClusterState lastKnownClusterState;
        private readonly Dictionary<Worker, long> lastKnownCut = new Dictionary<Worker, long>();
        private long maxVersion;
        private readonly DprFinderResponseParser parser = new DprFinderResponseParser();
        private readonly byte[] recvBuffer = new byte[1 << 20];

        public EnhancedDprFinder(Socket dprFinderConn)
        {
            this.dprFinderConn = dprFinderConn;
            dprFinderConn.NoDelay = true;
        }
        public EnhancedDprFinder(string ip, int port)
        {
            this.ip = ip;
            this.port = port;
            ResetUntilConnected();
            Console.WriteLine("CONNECTED");
        }

        private void ResetDprFinderConn()
        {
            EndPoint endpoint;
            if(Char.IsDigit(ip[0]))
            {
                endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            } else 
            {
                endpoint = new DnsEndPoint(ip, port);
            }
            dprFinderConn = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            dprFinderConn.NoDelay = true;
            dprFinderConn.Connect(endpoint);
        }

        private void ResetDprFinderConnSafe()
        {
            try
            {
                ResetDprFinderConn();
            } catch (Exception)
            {
                return;
            }
        }

        private void ResetUntilConnected()
        {
            bool connected = false;
            while(!connected)
            {
                try
                {
                    ResetDprFinderConn();
                    connected = true;
                } catch (Exception)
                {

                }
            }
        }

        public long SafeVersion(Worker worker)
        {
            return lastKnownCut.TryGetValue(worker, out var result) ? result : 0;
        }

        public IDprStateSnapshot GetStateSnapshot()
        {
            return new DictionaryDprStateSnapshot(lastKnownCut);
        }

        public long SystemWorldLine()
        {
            return lastKnownClusterState?.currentWorldLine ?? 1;
        }

        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        public void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (this)
            {
                dprFinderConn.SendNewCheckpointCommand(worldLine, persisted, deps);
                var received = dprFinderConn.ReceiveFailFast(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        public bool Refresh()
        {   
            lock (this)
            {
                try
                {
                    dprFinderConn.SendSyncCommand();
                    ProcessRespResponse();

                    maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                    var newState = ClusterState.FromBuffer(recvBuffer, parser.stringStart + sizeof(long), out var head);
                    Interlocked.Exchange(ref lastKnownClusterState, newState);
                    // Cut is unavailable, signal a resend.
                    if (BitConverter.ToInt32(recvBuffer, head) == -1) return false;
                    lock (lastKnownCut)
                    {
                        RespUtil.ReadDictionaryFromBytes(recvBuffer, head, lastKnownCut);
                    }
                } catch (SocketException) {
                    ResetUntilConnected();
                    return false;
                }
            }
            return true;
        }

        public Dictionary<Worker, (int, string)> FetchCluster() 
        {
            Dictionary<Worker, (int, string)> result;
            lock (this)
            {
                try
                {
                    dprFinderConn.SendFetchClusterCommand();
                    ProcessRespResponse();

                    result = ConfigurationResponse.FromBuffer(recvBuffer, parser.stringStart, out var head);
                    return result;
                } catch (SocketException) 
                {
                    ResetUntilConnected();
                    return FetchCluster();
                }
            }
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // No need to report recovery for enhanced DprFinder
        }

        public void ResendGraph(Worker worker, IStateObject stateObject)
        {
            lock (this)
            {
                try {
                    var acks = dprFinderConn.SendGraphReconstruction(worker, stateObject);
                    // Wait for all of the sent commands to be acked
                    var received = 0;
                    while (received < acks * 5) {
                        received += dprFinderConn.ReceiveFailFast(recvBuffer);
                    }
                } catch (SocketException)
                {
                    ResetUntilConnected();
                    ResendGraph(worker, stateObject);
                }
            }
        }

        public long NewWorker(WorkerInformation workerInfo, IStateObject stateObject)
        {
            if (stateObject != null)
                ResendGraph(workerInfo.worker, stateObject);
            lock (this)
            {
                try
                {
                    dprFinderConn.SendAddWorkerCommand(workerInfo);
                    ProcessRespResponse();
                    lastKnownClusterState ??= new ClusterState();
                    lastKnownClusterState.currentWorldLine = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                    return BitConverter.ToInt64(recvBuffer, parser.stringStart + sizeof(long));
                } catch (SocketException)
                {
                    ResetUntilConnected();
                    return NewWorker(workerInfo, stateObject);
                }
            }
        }

        public void DeleteWorker(Worker id)
        {
            lock (this)
            {
                dprFinderConn.SendDeleteWorkerCommand(id);
                var received = dprFinderConn.ReceiveFailFast(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0;
            while (true)
            {
                
                receivedSize += dprFinderConn.ReceiveFailFast(recvBuffer);
                for (; i < receivedSize; i++)
                    if (parser.ProcessChar(i, recvBuffer))
                        return;
            }
        }
    }
}