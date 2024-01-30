using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FASTER.libdpr.gRPC
{
    public class DprStatelessServerInterceptor : Interceptor
    {
        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var header = context.RequestHeaders.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            Debug.Assert(header != null);
            // Simply reflect the dependency information back
            var response = await continuation.Invoke(request, context);
            context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, header);
            return response;
        }
    }
}