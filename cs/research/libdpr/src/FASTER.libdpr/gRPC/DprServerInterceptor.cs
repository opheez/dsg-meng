using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FASTER.common;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FASTER.libdpr.gRPC
{
    public class DprServerInterceptor : Interceptor
    {
        private IDprWorker dprWorker;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;
        
        public DprServerInterceptor(IDprWorker dprWorker)
        {
            this.dprWorker = dprWorker;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var header = context.RequestHeaders.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            Debug.Assert(header != null);
            var status = dprWorker.TryReceive(header);
            switch (status)
            {
                case DprReceiveStatus.OK:
                    // Proceed with request
                    var response = await continuation.Invoke(request, context);
                    var buf = serializationArrayPool.Checkout();
                    dprWorker.Send(buf);
                    // TODO(Tianyu): Why no span variant?
                    context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
                    serializationArrayPool.Return(buf);
                    return response;
                case DprReceiveStatus.BUFFER:
                    // TODO(Tianyu): The plan here is to rely on client side to withhold messages that need to cross SU,
                    // and for the underlying dpr worker implementation to not buffer messages when applying rules for
                    // simplicity of server side handling. This decision may be revisited.
                    throw new NotImplementedException();
                case DprReceiveStatus.DISCARD:
                    // Use an error to signal to caller that this call cannot proceed
                    throw new RpcException(Status.DefaultCancelled);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

    }
}