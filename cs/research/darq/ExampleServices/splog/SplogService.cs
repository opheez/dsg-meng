using Grpc.Core;
using protobuf;

namespace ExampleServices.splog;

public class SplogService : protobuf.SplogService.SplogServiceBase
{
    private SpeculativeLog backend;

    public SplogService(SpeculativeLog backend)
    {
        this.backend = backend;
    }
    
    public override Task<SplogAppendResponse> Append(SplogAppendRequest request, ServerCallContext context)
    {
        var lsn = backend.log.Enqueue(request.Entry.Span);
        backend.EndAction();
        return Task.FromResult(new SplogAppendResponse
        {
            Ok = true,
            Lsn = lsn
        });
    }

    public override Task Scan(SplogScanRequest request, IServerStreamWriter<SplogEntry> responseStream, ServerCallContext context)
    {
    }

    public override Task<SplogTruncateResponse> Truncate(SplogTruncateRequest request, ServerCallContext context)
    {
    }
}