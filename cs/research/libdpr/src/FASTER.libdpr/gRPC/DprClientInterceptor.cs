using System;
using System.Net;
using FASTER.common;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FASTER.libdpr.gRPC
{
    public class DprClientInterceptor : Interceptor
    {
        private IDprWorker dprWorker;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;

        public DprClientInterceptor(IDprWorker dprWorker)
        {
            this.dprWorker = dprWorker;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
            BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            // TODO(Tianyu): Which idiot designed this such that there's no way to get headers in this variant, but possible to do so in others?
            throw new NotImplementedException();
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            var buffer = serializationArrayPool.Checkout();
            dprWorker.Send(buffer);

            var headers = context.Options.Headers;
            if (headers == null)
            {
                // TODO(Tianyu): Is this object expensive?
                headers = new Metadata();
                var options = context.Options.WithHeaders(headers);
                context = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, options);
            }
            // TODO(Tianyu): Why no span variant?
            headers.Add(DprMessageHeader.GprcMetadataKeyName, buffer);
            // TODO(Tianyu): Assuming it is ok now to return into object pool?
            serializationArrayPool.Return(buffer);
            
            // TODO(Tianyu): implement SU logic here
            //
            new AsyncUnaryCall<TResponse>(continued.)
            return continuation.Invoke(request, context);        
        }
        
        
    }
}