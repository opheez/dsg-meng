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
        // TODO(Tianyu): ugly interface?
        DprReceiveStatus TryReceive<TMessage>(Span<byte> header, TMessage m, out Task<TMessage> onReceivable) where TMessage : class;

        int Send(Span<byte> outputHeaderBytes);
        
        void Refresh();
    }
}