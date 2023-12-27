using System;
using System.Threading.Tasks;

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
        DprReceiveStatus TryReceive(Span<byte> header, out Task onReceivable);

        int Send(Span<byte> outputHeaderBytes);
        
        void Refresh();
    }
}