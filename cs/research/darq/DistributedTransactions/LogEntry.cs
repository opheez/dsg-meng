
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace DB {

public enum LogType {
    Begin,
    Write,
    Commit,
    Abort,
    // used for 2pc, doesn't use LSN/prevLSN until being logged as a self message
    Prepare,
    Ok,
    Ack
}

public struct LogEntry{
    public bool persited = false;
    public long lsn; // value of DarqId for Ack messages
    public long prevLsn; // do we even need this if we are undoing? for Ack messages, this is original tid
    public long tid;
    public LogType type;
    public KeyAttr[] keyAttrs;
    public byte[][] vals;
    public static readonly int MinSize = sizeof(long) * 3 + sizeof(int);
    public LogEntry(long prevLsn, long tid, KeyAttr keyAttr, byte[] val){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = LogType.Write;
        this.keyAttrs = new KeyAttr[1]{keyAttr};
        this.vals = new byte[][]{val};
    }
    public LogEntry(long prevLsn, long tid, KeyAttr[] keyAttr, byte[][] val){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = LogType.Prepare;
        this.keyAttrs = keyAttr;
        this.vals = val;
    }

    public LogEntry(long prevLsn, long tid, LogType type){
        this.prevLsn = prevLsn;
        this.tid = tid;
        this.type = type;
    }

    public void SetPersisted(){
        persited = true;
    }

    public unsafe byte[] ToBytes(){
        int totalSize = MinSize + (vals != null ? sizeof(int) : 0);
        if (vals != null) for (int i = 0; i < vals.Length; i++) totalSize += vals[i].Length + sizeof(int) * 2 + keyAttrs[i].Size;

        byte[] arr = new byte[totalSize];

        fixed (byte* b = arr) {
            var head = b;
            *(long*)head = lsn;
            head += sizeof(long);
            *(long*)head = prevLsn;
            head += sizeof(long);
            *(long*)head = tid;
            head += sizeof(long);
            *(int*)head = (int)type;
            head += sizeof(int);
            if (type == LogType.Write || type == LogType.Prepare){
                Debug.Assert(keyAttrs.Length == vals.Length);
                *(int*)head = keyAttrs.Length;
                head += sizeof(int);
                for (int i = 0; i < keyAttrs.Length; i++){
                    *(int*)head = keyAttrs[i].Size;
                    head += sizeof(int);
                    keyAttrs[i].ToBytes().CopyTo(new Span<byte>(head, keyAttrs[i].Size));
                    head += keyAttrs[i].Size;
                    *(int*)head = vals[i].Length;
                    head += sizeof(int);
                    vals[i].CopyTo(new Span<byte>(head, vals[i].Length));
                    head += vals[i].Length;
                }
            }
        }

        return arr;
    }

    public static unsafe LogEntry FromBytes(byte[] data) {
        // Ensure that the data array has enough bytes for the struct
        if (data.Length < MinSize) throw new ArgumentException("Insufficient data to deserialize the struct.");

        LogEntry result = new LogEntry();

        fixed (byte* b = data) {
            var head = b;
            result.lsn = *(long*)head;
            head += sizeof(long);
            result.prevLsn = *(long*)head;
            head += sizeof(long);
            result.tid = *(long*)head;
            head += sizeof(long);
            result.type = (LogType)(*(int*)head);
            head += sizeof(int);
            if (result.type == LogType.Write || result.type == LogType.Prepare){
                int len = *(int*)head;
                head += sizeof(int);
                result.keyAttrs = new KeyAttr[len];
                result.vals = new byte[len][];
                for (int i = 0; i < len; i++){
                    int keySize = *(int*)head;
                    head += sizeof(int);
                    result.keyAttrs[i] = KeyAttr.FromBytes(new Span<byte>(head, keySize).ToArray());
                    head += keySize;
                    int valSize = *(int*)head;
                    head += sizeof(int);
                    result.vals[i] = new byte[valSize];
                    new Span<byte>(head, valSize).CopyTo(result.vals[i]);
                    head += valSize;
                }
            }
        }

        return result;
    }

    public override readonly bool Equals(object? obj)
    {
        if (obj == null || GetType() != obj.GetType())
        {
            return false;
        }
        LogEntry o = (LogEntry)obj;
        if (vals.Length != o.vals.Length) return false;
        for (int i = 0; i < vals.Length; i++){
            if (!vals[i].AsSpan().SequenceEqual(o.vals[i])) return false;
        }
        return o.lsn == lsn && o.prevLsn == prevLsn && o.tid == tid && o.type == type && o.keyAttrs.SequenceEqual(keyAttrs);
    }

    public override readonly string ToString(){
        return $"LogEntry(lsn={lsn}, prevLsn={prevLsn}, tid={tid}, type={type}, keyAttr={keyAttrs}, val={vals})";
    }
    
}
}