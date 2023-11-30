using System;

namespace FASTER.libdpr
{
    public enum DprReceiveStatus
    {
        OK,
        BUFFER,
        DISCARD
    }
    
    public interface IDprWorker
    {
        DprReceiveStatus TryReceive(Span<byte> header);

        void Send(Span<byte> outputHeaderBytes);
        
        void Refresh();
    }
}