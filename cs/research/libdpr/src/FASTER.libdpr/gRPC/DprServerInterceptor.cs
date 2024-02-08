using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Status = Grpc.Core.Status;

namespace FASTER.libdpr.gRPC
{
    public class DprServerInterceptor<TStateObject> : Interceptor where TStateObject : IStateObject
    {
        private DprWorker<TStateObject, RwLatchVersionScheme> dprWorker;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;
        
        // For now, require that the gRPC integration only works with RwLatchVersionScheme, which supports protected
        // blocks that start and end on different threads
        public DprServerInterceptor(DprWorker<TStateObject, RwLatchVersionScheme> dprWorker)
        {
            this.dprWorker = dprWorker;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var header = context.RequestHeaders.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            Debug.Assert(header != null);
            if (!dprWorker.StartReceiveAction(header))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            return await HandleCall(request, context, continuation);
        }

        private async Task<TResponse> HandleCall<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation) where TRequest : class where TResponse : class
        {
            // Proceed with request
            var response = await continuation.Invoke(request, context);
            var buf = serializationArrayPool.Checkout();
            dprWorker.EndActionAndProduceTag(buf);
            // TODO(Tianyu): Add SU handling logic here to await for the response to become committed
            context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
            serializationArrayPool.Return(buf);
            return response;
        } 
    }
}