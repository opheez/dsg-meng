using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    public static class Utility
    {
#if NETSTANDARD2_1
        public static bool TryWriteBytes(Span<byte> destination, long value) => BitConverter.TryWriteBytes(destination, value);
        
        public static bool TryWriteBytes(Span<byte> destination, int value) => BitConverter.TryWriteBytes(destination, value);
        
        public static IEnumerable<T> Append<T>(this IEnumerable<T> src, T elem) => src.Append(elem);
        
#else
        
        // TODO(TIanyu): Performance issue and should be fixed
        public static IEnumerable<T> Append<T>(this IEnumerable<T> src, T elem)
        {
            var l = src.ToList();
            l.Add(elem);
            return l;
        }

        public static unsafe bool TryWriteBytes(Span<byte> destination, long value)
        {
            if (destination.Length < sizeof(long)) return false;
            fixed (byte* bp = destination)
            {
                Unsafe.Write(bp, value);
            }

            return true;
        }

        public static unsafe bool TryWriteBytes(Span<byte> destination, int value)
        {
            if (destination.Length < sizeof(int)) return false;
            fixed (byte* bp = destination)
            {
                Unsafe.Write(bp, value);
            }

            return true;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key,
            TValue defaultValue)
        {
            return dictionary.TryGetValue(key, out var result) ? result : defaultValue;
        }

        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key)) return false;
            dictionary.Add(key, value);
            return true;
        }
#endif

        private static readonly bool debugging = true;
        private static readonly bool distributed = true;
        public static int ReceiveFailFast(this Socket conn, byte[] buffer)
        {
            int result = conn.Receive(buffer);
            if(result == 0)
                throw new SocketException(32);
            return result;
        }

        private static void Write(string file, string text)
        {
                using (TextWriter tw = TextWriter.Synchronized(File.AppendText(file)))
                {
                    tw.WriteLine(text);
                }
        }

        public static void LogDebug(string file, string text)
        {
            if(debugging && distributed)
            {
                Write(file, text);
            }
        }

        public static void LogBasic(string file, string text)
        {
            if(distributed)
                Write(file, text);
        }
    }
}