using System;
using System.IO;
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
        private static readonly string debugLogFile = "/DprCounters/data/serverLog.txt";
        private static readonly byte[] OkResponse = Encoding.GetEncoding("ASCII").GetBytes("+OK\r\n");
        private readonly EnhancedDprFinderBackend backend;
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
        }

        /// <summary>
        ///     Main server loop for DPR finding
        /// </summary>
        public void StartServer()
        {
            Extensions.LogDebug(debugLogFile, "##############\n\nSERVER STARTED");
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

        private void FailFast()
        {   // easy way to replicate failing during recovery at any given message below
            string path = "/DprCounters/data/failing.txt";
            if(File.Exists(path))
                return;
            else
            {
                using(FileStream fs = File.Create(path))
                {
                    Environment.Exit(1);
                }
            }
        }

        private void HandleClientCommand(DprFinderCommand command, Socket socket)
        {
            switch (command.commandType)
            {
                case DprFinderCommand.Type.NEW_CHECKPOINT:
                    backend.NewCheckpoint(command.worldLine, command.wv, command.deps);
                    // Ack immediately as the graph is not required to be fault-tolerant
                    socket.Send(OkResponse);
                    break;
                case DprFinderCommand.Type.GRAPH_RESENT:
                    // FailFast();
                    backend.MarkWorkerAccountedFor(command.wv.Worker, command.wv.Version);
                    socket.Send(OkResponse);
                    break;
                case DprFinderCommand.Type.SYNC:
                    var precomputedResponse = backend.GetPrecomputedResponse();
                    precomputedResponse.rwLatch.EnterReadLock();
                    socket.SendSyncResponse(backend.MaxVersion(),
                        ValueTuple.Create(precomputedResponse.serializedResponse, precomputedResponse.responseEnd));
                    precomputedResponse.rwLatch.ExitReadLock();
                    break;
                case DprFinderCommand.Type.ADD_WORKER:
                    backend.AddWorker(command.wi, socket.SendAddWorkerResponse);
                    break;
                case DprFinderCommand.Type.DELETE_WORKER:
                    backend.DeleteWorker(command.w, () => socket.Send(OkResponse));
                    break;
                case DprFinderCommand.Type.FETCH_CLUSTER:
                    var fetchedCluster = backend.GetClusterState();
                    fetchedCluster.rwLatch.EnterReadLock();
                    socket.SendFetchClusterResponse(ValueTuple.Create(fetchedCluster.serializedResponse, fetchedCluster.responseEnd));
                    fetchedCluster.rwLatch.ExitReadLock();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}