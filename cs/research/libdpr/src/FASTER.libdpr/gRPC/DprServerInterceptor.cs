using System.Threading.Tasks;
using FASTER.common;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Status = Grpc.Core.Status;

namespace FASTER.libdpr.gRPC
{
    public class DprServerInterceptor<TStateObject, TService> : Interceptor where TStateObject : IStateObject
    {
        private DprWorker<TStateObject> dprWorker;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;

        // For now, we require that the gRPC integration only works with RwLatchVersionScheme, which supports protected
        // blocks that start and end on different threads
        // TService is a parameter for DI to only create interceptors after the service is up 
        public DprServerInterceptor(DprWorker<TStateObject> dprWorker, TService service)
        {
            this.dprWorker = dprWorker;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var header = context.RequestHeaders.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            if (header != null)
            {
                // Speculative code path
                if (!dprWorker.TryReceiveAndStartAction(header))
                    // Use an error to signal to caller that this call cannot proceed
                    // TODO(Tianyu): add more descriptive exception information
                    throw new RpcException(Status.DefaultCancelled);
                return await HandleSpeculativeCall(request, context, continuation);
            }

            // Non speculative code path
            dprWorker.StartLocalAction();
            return await HandleNonSpeculativeCall(request, context, continuation);
        }

        private async Task<TResponse> HandleNonSpeculativeCall<TRequest, TResponse>(TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation) where TRequest : class where TResponse : class
        {
            // Proceed with request
            var response = await continuation.Invoke(request, context);
            dprWorker.EndAction();
            await dprWorker.NextCommit();
            return response;
        }

        private async Task<TResponse> HandleSpeculativeCall<TRequest, TResponse>(TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation) where TRequest : class where TResponse : class
        {
            // Proceed with request
            var response = await continuation.Invoke(request, context);
            var buf = serializationArrayPool.Checkout();
            dprWorker.ProduceTagAndEndAction(buf);
            // TODO(Tianyu): Add SU handling logic here to await for the response to become committed
            context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
            serializationArrayPool.Return(buf);
            return response;
        }
    }
}