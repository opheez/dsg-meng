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
                    // Handle request on the spot if ok
                    return await HandleCall(request, context, continuation);
                case DprReceiveStatus.DISCARD:
                    // Use an error to signal to caller that this call cannot proceed
                    // TODO(Tianyu): add more descriptive exception information
                    throw new RpcException(Status.DefaultCancelled);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task<TResponse> HandleCall<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation) where TRequest : class where TResponse : class
        {
            // Proceed with request
            var response = await continuation.Invoke(request, context);
            var buf = serializationArrayPool.Checkout();
            dprWorker.TagMessage(buf);
            // TODO(Tianyu): Add SU handling logic here to await for the response to become committed
            context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
            serializationArrayPool.Return(buf);
            return response;
        } 
    }
}