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
    public static class Extensions
    {
        private static readonly bool debugging = true;
        public static int ReceiveFailFast(this Socket conn, byte[] buffer)
        {
            int result = conn.Receive(buffer);
            if(result == 0)
                throw new SocketException(32);
            return result;
        }

        private static void Write(string file, string text)
        {
                using (TextWriter tw = TextWriter.Synchronized(File.AppendText(file)))
                {
                    tw.WriteLine(text);
                }
        }

        public static void LogDebug(string file, string text)
        {
            if(debugging)
            {
                Write(file, text);
            }
        }

        public static void LogBasic(string file, string text)
        {
            Write(file, text);
        }
    }
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
                var received = dprFinderConn.ReceiveFailFast(recvBuffer);
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
                } catch (SocketException e) {
                    try
                    {
                        ResetDprFinderConn();
                        return false;
                    } catch (Exception ee)
                    {
                        return false;
                    }
                }
            }
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
                    received += dprFinderConn.ReceiveFailFast(recvBuffer);
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