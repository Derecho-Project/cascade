using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

/// <summary>
/// Namespace containing UDL implementer types and a wrapper type for
/// OCDPO arguments received from unmanaged memory.
/// </summary>
namespace GatewayLib
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct OcdpoArgsInternal
    {
        public UInt32 sender;
        public IntPtr object_pool_pathname;
        public IntPtr key_string;
        public IntPtr object_key;
        public IntPtr object_bytes;
        public UInt64 object_bytes_size;
        public UInt32 worker_id;
        public IntPtr emit_func;
    }

    public delegate void InvokeEmit(IntPtr emitFnPtr, IntPtr key, IntPtr bytes, UInt32 size);
} // namespace GatewayLib
