using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A simple single-server DprFinder implementation relying primarily on graph traversal.
    ///     Fault-tolerant in that all reported commits are persisted on a given IDevice and a new SimpleDprFinderServer
    ///     can restart from persisted state of a failed one to appear as if it never failed.
    ///     The server speaks the Redis protocol and appears as a Redis server that supports the following commands:\
    ///     AddWorker(worker) -> OK
    ///     RemoveWorker(worker) -> OK
    ///     NewCheckpoint(wv, deps) -> OK
    ///     ReportRecovery(wv, worldLine) -> OK
    ///     Sync() -> state
    ///     All parameters and return values are Redis bulk strings of bytes that encode the corresponding C#
    ///     object with the exception of return values of '+OK\r\n's
    /// </summary>
    public class EnhancedDprFinderServer : IDisposable
    {
        private static readonly byte[] OkResponse = Encoding.GetEncoding("ASCII").GetBytes("+OK\r\n");

        private object testLock = new object();
        private readonly EnhancedDprFinderBackend backend;
        private readonly ClusterBackend clusterBackend;
        private readonly string ip;
        private readonly int port;
        private Thread processThread;
        private Socket servSocket;
        private ManualResetEventSlim termination;

        /// <summary>
        ///     Constructs a new SimpleDprFinderServer instance at the given ip, listening on the given port,
        ///     and using the given backend object
        /// </summary>
        /// <param name="ip">ip address of server</param>
        /// <param name="port">port to listen on the server</param>
        /// <param name="backend">backend of the server</param>
        public EnhancedDprFinderServer(string ip, int port, EnhancedDprFinderBackend backend)
        {
            this.ip = ip;
            this.port = port;
            this.backend = backend;
        }

        public EnhancedDprFinderServer(string ip, int port, EnhancedDprFinderBackend backend, ClusterBackend clusterBackend)
        {
            this.ip = ip;
            this.port = port;
            this.backend = backend;
            this.clusterBackend = clusterBackend;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            servSocket.Dispose();
            // TODO(Tianyu): Clean shutdown of client connections

            termination.Set();
            processThread.Join();
            if(backend != null)
            {
                backend.Dispose();
            }
            if(clusterBackend != null)
            {
                // clusterBackend.Dispose();
            }
        }

        /// <summary>
        ///     Main server loop for DPR finding
        /// </summary>
        public void StartServer()
        {
            termination = new ManualResetEventSlim();

            processThread = new Thread(() =>
            {
                while (!termination.IsSet)
                    if(backend != null)
                        backend.Process();
            });
            processThread.Start();

            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            servSocket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);

            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private bool HandleNewClientConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                Console.WriteLine("DISPOSED OF STUFF");
                return false;
            }

            e.AcceptSocket.NoDelay = true;
            // Set up listening events
            var saea = new SocketAsyncEventArgs();
            saea.SetBuffer(new byte[BatchInfo.MaxHeaderSize], 0, BatchInfo.MaxHeaderSize);
            saea.UserToken = new MessageUtil.DprFinderRedisProtocolConnState(e.AcceptSocket, HandleClientCommand);
            saea.Completed += MessageUtil.DprFinderRedisProtocolConnState.RecvEventArg_Completed;
            // If the client already have packets, avoid handling it here on the handler thread so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(saea))
                Task.Run(() => MessageUtil.DprFinderRedisProtocolConnState.RecvEventArg_Completed(null, saea));
            return true;
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            do
            {
                if (!HandleNewClientConnection(e)) break;
                e.AcceptSocket = null;
            } while (!servSocket.AcceptAsync(e));
        }

        private void HandleClientCommand(DprFinderCommand command, Socket socket)
        {
            switch (command.commandType)
            {
                case DprFinderCommand.Type.NEW_CHECKPOINT:
                    // Console.WriteLine("NEW CHECKPOINT RECEIVED");
                    backend.NewCheckpoint(command.worldLine, command.wv, command.deps);
                    // Ack immediately as the graph is not required to be fault-tolerant
                    socket.Send(OkResponse);
                    // Console.WriteLine("RESPONSE SENT");
                    break;
                case DprFinderCommand.Type.GRAPH_RESENT:
                    // Console.WriteLine("RECEIVING GRAPH RESENDS");
                    lock(testLock) // TODO(Nikola): probably not needed, leaving for now just in case
                    {
                        backend.MarkWorkerAccountedFor(command.wv.Worker, command.wv.Version);
                        socket.Send(OkResponse); // added this, it's probably the ack we need to unblock
                    }
                    break;
                case DprFinderCommand.Type.SYNC:
                    // Console.WriteLine("RECEIVING SYNCS");
                    var precomputedResponse = backend.GetPrecomputedResponse();
                    precomputedResponse.rwLatch.EnterReadLock();
                    // var max_version = backend.MaxVersion();
                    // Console.WriteLine("MAX VERSION: " + max_version.ToString());
                    socket.SendSyncResponse(backend.MaxVersion(),
                        ValueTuple.Create(precomputedResponse.serializedResponse, precomputedResponse.responseEnd));
                    precomputedResponse.rwLatch.ExitReadLock();
                    break;
                case DprFinderCommand.Type.ADD_WORKER:
                    // Console.WriteLine("RECEIVED ADD WORKER");
                    backend.AddWorker(command.w, socket.SendAddWorkerResponse);
                    break;
                case DprFinderCommand.Type.DELETE_WORKER:
                    // Console.WriteLine("DELETE WORKER RECEIVED");
                    backend.DeleteWorker(command.w, () => socket.Send(OkResponse));
                    break;
                case DprFinderCommand.Type.FETCH_CLUSTER:
                    // Console.WriteLine("FETCH CLUSTER RECEIVED");
                    var fetchedCluster = clusterBackend.getClusterState();
                    fetchedCluster.rwLatch.EnterReadLock();
                    socket.SendFetchClusterResponse(ValueTuple.Create(fetchedCluster.serializedResponse, fetchedCluster.responseEnd));
                    fetchedCluster.rwLatch.ExitReadLock();
                    break;
                default:
                    // Console.WriteLine("ARGUMENT EXCEPTION");
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}