using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.libdpr
{
    public class EnhancedDprFinder : IDprFinder
    {
        // private readonly Socket dprFinderConn;
        int refresh_number = 0;
        private Socket dprFinderConn;
        object dprFinderConnLock;
        string ip;
        int port;
        private ClusterState lastKnownClusterState;
        private readonly Dictionary<Worker, long> lastKnownCut = new Dictionary<Worker, long>();
        private long maxVersion;
        private readonly DprFinderResponseParser parser = new DprFinderResponseParser();
        private readonly byte[] recvBuffer = new byte[1 << 20];

        public EnhancedDprFinder(Socket dprFinderConn)
        {
            this.dprFinderConn = dprFinderConn;
            this.dprFinderConnLock = true;
            dprFinderConn.NoDelay = true;
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
            // var ipEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            dprFinderConn = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // dprFinderConn = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            dprFinderConn.NoDelay = true;
            dprFinderConn.Connect(endpoint);
        }
        public EnhancedDprFinder(string ip, int port)
        {
            this.ip = ip;
            this.port = port;
            this.dprFinderConnLock = true;
            ResetDprFinderConn();
            // EndPoint endpoint;
            // if(Char.IsDigit(ip[0]))
            // {
            //     endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            // } else 
            // {
            //     endpoint = new DnsEndPoint(ip, port);
            // }
            // // var ipEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            // dprFinderConn = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // // dprFinderConn = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            // dprFinderConn.NoDelay = true;
            // dprFinderConn.Connect(endpoint);
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
            lock (dprFinderConnLock)
            {
                dprFinderConn.SendNewCheckpointCommand(worldLine, persisted, deps);
                // Console.WriteLine("WAITING TO RECEIVE");
                var received = dprFinderConn.Receive(recvBuffer);
                // Console.WriteLine("RECEIVED");
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        public bool Refresh()
        {   
            refresh_number++;
            lock (dprFinderConnLock)
            {
                try
                {
                    // Console.WriteLine("SENDING SYNC");
                    dprFinderConn.SendSyncCommand();
                    // Console.WriteLine("DONE SYNC");
                    ProcessRespResponse();
                    // Console.WriteLine("DONE PROCESSING");

                    maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                    var newState = ClusterState.FromBuffer(recvBuffer, parser.stringStart + sizeof(long), out var head);
                    Interlocked.Exchange(ref lastKnownClusterState, newState);
                    // Console.WriteLine("DONE EXCHANGING");
                    // Cut is unavailable, signal a resend.
                    if (BitConverter.ToInt32(recvBuffer, head) == -1) return false;
                    lock (lastKnownCut)
                    {
                        // Console.WriteLine("STARTING READING");
                        RespUtil.ReadDictionaryFromBytes(recvBuffer, head, lastKnownCut);
                        // Console.WriteLine("DONE READING");
                    }
                } catch (SocketException e) {
                    try
                    {
                        // Console.WriteLine("CONNECTION RESET: " + refresh_number.ToString());
                        ResetDprFinderConn();
                        Console.WriteLine("CONNECTION RESET SUCCESSFULLY");
                        // return Refresh();
                        return false;
                    } catch (Exception ee)
                    {
                        // Console.WriteLine("RETURNING FALSE");
                        return false;
                    }
                }
            }
            // Console.WriteLine("REFRESH DONE: " + refresh_number.ToString());
            return true;
        }

        public Dictionary<Worker, EndPoint> FetchCluster() 
        {
            Dictionary<Worker, EndPoint> result;
            lock (dprFinderConnLock)
            {
                dprFinderConn.SendFetchClusterCommand();
                ProcessRespResponse();

                result = ConfigurationResponse.FromBuffer(recvBuffer, parser.stringStart, out var head);
            }
            return result;
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // No need to report recovery for enhanced DprFinder
        }

        public void ResendGraph(Worker worker, IStateObject stateObject)
        {
            lock (dprFinderConnLock)
            {
                var acks = dprFinderConn.SendGraphReconstruction(worker, stateObject);
                // Wait for all of the sent commands to be acked
                var received = 0;
                while (received < acks * 5) {
                    received += dprFinderConn.Receive(recvBuffer);
                }
            }
        }

        public long NewWorker(Worker id, IStateObject stateObject)
        {
            if (stateObject != null) // Why is this necessary? It is never null (with the current code)
                ResendGraph(id, stateObject); // This breaks something inside the Dpr Finder backend somehow
            lock (dprFinderConnLock)
            {
                dprFinderConn.SendAddWorkerCommand(id);
                ProcessRespResponse();
                lastKnownClusterState ??= new ClusterState();
                lastKnownClusterState.currentWorldLine = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                return BitConverter.ToInt64(recvBuffer, parser.stringStart + sizeof(long));
            }
        }

        public void DeleteWorker(Worker id)
        {
            lock (dprFinderConnLock)
            {
                dprFinderConn.SendDeleteWorkerCommand(id);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0, zeroByteFails = 0, zeroByteTolerance = 10;
            // while (true)
            while(zeroByteFails < zeroByteTolerance)
            {
                // Console.WriteLine("WHILE AGAIN");
                var additionalSize = dprFinderConn.Receive(recvBuffer);
                receivedSize += additionalSize;
                if(additionalSize == 0) {
                    Thread.Sleep(10);
                    zeroByteFails++;
                }
                // Console.WriteLine("RECEIVED");
                for (; i < receivedSize; i++)
                    if (parser.ProcessChar(i, recvBuffer))
                        return;
                    // else
                    //     Console.WriteLine("PROCESSED");
            }
            throw new SocketException(32);
        }
    }
}