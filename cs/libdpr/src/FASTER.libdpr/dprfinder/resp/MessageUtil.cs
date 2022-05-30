using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace FASTER.libdpr
{
    internal static class MessageUtil
    {
        private static readonly bool debugging = false;
        private static readonly string serverLog = "/DprCounters/data/server.txt";
        private static readonly ThreadLocalObjectPool<byte[]> reusableMessageBuffers =
            new ThreadLocalObjectPool<byte[]>(() => new byte[BatchInfo.MaxHeaderSize], 1);

        internal static int SendGraphReconstruction(this Socket socket, Worker worker, IStateObject stateObject)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            var checkpoints = stateObject.GetUnprunedVersions();
            var minVersion = long.MaxValue;
            var numRequests = 0;
            foreach (var (bytes, offset) in checkpoints)
            {
                SerializationUtil.DeserializeCheckpointMetadata(bytes, offset,
                    out var worldLine, out var wv, out var deps);
                head += RespUtil.WriteRedisArrayHeader(4, buf, head);
                head += RespUtil.WriteRedisBulkString("NewCheckpoint", buf, head);
                head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
                head += RespUtil.WriteRedisBulkString(wv, buf, head);
                head += RespUtil.WriteRedisBulkString(deps, buf, head);
                if (minVersion > wv.Version) minVersion = wv.Version;
                numRequests++;
            }
            if (numRequests == 0) return 0;
            head += RespUtil.WriteRedisArrayHeader(2, buf, head);
            head += RespUtil.WriteRedisBulkString("GraphResent", buf, head);
            var committedVersion = new WorkerVersion(worker, minVersion == long.MaxValue ? 0 : minVersion);
            head += RespUtil.WriteRedisBulkString(committedVersion, buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nNew NewCheckpoint Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            return ++numRequests;
        }

        internal static void SendAddWorkerCommand(this Socket socket, Worker worker)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(2, buf, 0);
            head += RespUtil.WriteRedisBulkString("AddWorker", buf, head);
            head += RespUtil.WriteRedisBulkString(worker.guid, buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nNew Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendDeleteWorkerCommand(this Socket socket, Worker worker)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(2, buf, 0);
            head += RespUtil.WriteRedisBulkString("DeleteWorker", buf, head);
            head += RespUtil.WriteRedisBulkString(worker.guid, buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nRequest id: \nNew Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            Extensions.LogDebug(serverLog, "Delete Worker sent");
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendNewCheckpointCommand(this Socket socket, long worldLine, WorkerVersion checkpointed,
            IEnumerable<WorkerVersion> deps)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(4, buf, 0);
            head += RespUtil.WriteRedisBulkString("NewCheckpoint", buf, head);
            head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
            head += RespUtil.WriteRedisBulkString(checkpointed, buf, head);
            head += RespUtil.WriteRedisBulkString(deps, buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nNew Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendReportRecoveryCommand(this Socket socket, WorkerVersion recovered,
            long worldLine)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(3, buf, 0);
            head += RespUtil.WriteRedisBulkString("ReportRecovery", buf, head);
            head += RespUtil.WriteRedisBulkString(recovered, buf, head);
            head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nNew Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendFetchClusterCommand(this Socket socket)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(1, buf, 0);
            head += RespUtil.WriteRedisBulkString("FetchCluster", buf, head);
            string requestSent = Encoding.ASCII.GetString(buf, 0, head);
            Extensions.LogDebug(serverLog, String.Format("#######\nNew Request:\n{0}", requestSent));
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendSyncCommand(this Socket socket)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(1, buf, 0);
            head += RespUtil.WriteRedisBulkString("Sync", buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendFetchClusterResponse(this Socket socket, (byte[], int) serializedState) 
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            buf[head++] = (byte) '$';

            var size = RespUtil.LongToDecimalString(serializedState.Item2, buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + serializedState.Item2 < buf.Length);
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            Array.Copy(serializedState.Item1, 0, buf, head, serializedState.Item2);
            head += serializedState.Item2;

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendSyncResponse(this Socket socket, long maxVersion, (byte[], int) serializedState)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            buf[head++] = (byte) '$';

            var size = RespUtil.LongToDecimalString(sizeof(long) + serializedState.Item2, buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + sizeof(long) + serializedState.Item2 < buf.Length);
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), maxVersion);
            head += sizeof(long);
            Array.Copy(serializedState.Item1, 0, buf, head, serializedState.Item2);
            head += serializedState.Item2;

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendAddWorkerResponse(this Socket socket, (long, long) result)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            buf[head++] = (byte) '$';

            var size = RespUtil.LongToDecimalString(2 * sizeof(long), buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + 2 * sizeof(long) < buf.Length);
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), result.Item1);
            head += sizeof(long);
            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), result.Item2);
            head += sizeof(long);

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        // TODO(Tianyu): Eliminate ad-hoc serialization code and move this inside WorkerVersion class

        internal class DprFinderRedisProtocolConnState
        {
            private static readonly string connStateLog = "/DprCounters/data/serverLog.txt";
            private readonly Action<DprFinderCommand, Socket> commandHandler;
            private readonly DprFinderCommandParser parser = new DprFinderCommandParser();
            private int readHead, bytesRead, commandStart;
            private readonly Socket socket;

            internal DprFinderRedisProtocolConnState(Socket socket, Action<DprFinderCommand, Socket> commandHandler)
            {
                this.socket = socket;
                this.commandHandler = commandHandler;
            }

            private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
            {
                var connState = (DprFinderRedisProtocolConnState) e.UserToken;
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    connState.socket.Dispose();
                    e.Dispose();
                    return false;
                }

                connState.bytesRead += e.BytesTransferred;
                string receivedFrom = ((IPEndPoint)connState.socket.RemoteEndPoint).Address.ToString();
                string receivedBufferRaw = Encoding.ASCII.GetString(e.Buffer, connState.readHead, connState.bytesRead - connState.readHead);
                Extensions.LogDebug(connStateLog, String.Format("##########\nSender:{0}\nMessage Received:\n{1}", receivedFrom, receivedBufferRaw));
                for (; connState.readHead < connState.bytesRead; connState.readHead++)
                {
                    if (connState.parser.ProcessChar(connState.readHead, e.Buffer))
                    {
                        Extensions.LogDebug(connStateLog, "Full Message Found");
                        connState.commandHandler(connState.parser.currentCommand, connState.socket);
                        connState.commandStart = connState.readHead + 1;
                    }
                }

                // TODO(Tianyu): Magic number
                // If less than some certain number of bytes left in the buffer, shift buffer content to head to free
                // up some space. Don't want to do this too often. Obviously ok to do if no bytes need to be copied (
                // the current end of buffer marks the end of a command, and we can discard the entire buffer).
                if (e.Buffer.Length - connState.readHead < 4096 || connState.readHead == connState.commandStart)
                {
                    var bytesLeft = connState.bytesRead - connState.commandStart;
                    // Shift buffer to front
                    Array.Copy(e.Buffer, connState.commandStart, e.Buffer, 0, bytesLeft);
                    connState.bytesRead = bytesLeft;
                    connState.readHead -= connState.commandStart;
                    connState.commandStart = 0;
                }

                e.SetBuffer(connState.readHead, e.Buffer.Length - connState.readHead);
                return true;
            }

            internal static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
            {
                var connState = (DprFinderRedisProtocolConnState) e.UserToken;
                try
                {
                    do
                    {
                        // No more things to receive
                        if (!HandleReceiveCompletion(e)) return;
                    } while (!connState.socket.ReceiveAsync(e));
                }
                catch (ObjectDisposedException)
                {
                    // Probably caused by a normal cancellation from this side. Ok to ignore
                }
            }
        }
    }
}