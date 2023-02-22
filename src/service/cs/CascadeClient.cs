using System;
using System.Runtime.InteropServices;

namespace Derecho.Cascade
{
    public unsafe class CascadeClient
    {
        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct ObjectProperties
        {
            // string key. Using IntPtr to avoid a marshalling overhead.
            public IntPtr key;
            public IntPtr bytes;
            public UInt64 bytes_size;
            public Int64 version;
            public UInt64 timestamp;
            public Int64 previous_version;
            public Int64 previous_version_by_key;
            public UInt64 message_id;
        }

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern ObjectProperties invoke_get_result(IntPtr results);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        private static extern IntPtr get(IntPtr capi, string key);

        public static ObjectProperties do_get(IntPtr capi, string key) {
            IntPtr results = get(capi, key);
            return invoke_get_result(results);
        }
        
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static unsafe extern IntPtr put(IntPtr capi, string object_pool_path, IntPtr data,
            UInt64 data_size, UInt32 subgroup_index, UInt32 shard_index);
        
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern IntPtr get_service_client_ref();

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern UInt32 get_subgroup_index_vcss(IntPtr capi);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern UInt32 get_my_id(IntPtr capi);

        public static void Main()
        {
            IntPtr capi = get_service_client_ref();
            var subgroupIndex = get_subgroup_index_vcss(capi);
            var nodeId = get_my_id(capi);

            Console.WriteLine("Subgroup index: " + subgroupIndex);
            Console.WriteLine("Node id: " + nodeId);

            IntPtr bytesPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            Int32 val = 1;
            Marshal.StructureToPtr(val, bytesPtr, false);        
            put(capi, "/console_printer/obj_a", bytesPtr, (UInt64) Marshal.SizeOf(typeof(Int32)), 0, 0);

            Console.WriteLine("Enter key of obj: ");
            string key = Console.ReadLine();

            ObjectProperties objResult = do_get(capi, key);
            Console.WriteLine("Result:");
            Console.WriteLine("Version: " + objResult.version);
            Console.WriteLine("Data as string: " + Marshal.PtrToStringAuto(objResult.bytes, (Int32) objResult.bytes_size));

        }
    }
} // namespace Derecho.Cascade
