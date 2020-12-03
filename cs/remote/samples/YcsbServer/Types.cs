﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace YcsbServer
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key : IFasterEqualityComparer<Key>
    {
        [FieldOffset(0)]
        public long value;


        public override string ToString()
        {
            return "{ " + value + " }";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref Key k)
        {
            return Utility.GetHashCode(k.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.value == k2.value;
        }

    }

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Value
    {
        [FieldOffset(0)]
        public long value;
    }

    public struct Input
    {
        public long value;
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct Output
    {
        [FieldOffset(0)]
        public Value value;
    }


    public struct Functions : IFunctions<Key, Value, Input, Output, long>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, long ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value value, long ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, long ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.value = input.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.value = input.value + oldValue.value;
        }
    }

}
