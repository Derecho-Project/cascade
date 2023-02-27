using System;
using System.Runtime.InteropServices;

/// <summary>
/// This library contains utilities for accessing the Cascade external service client API.
/// These functions are exposed from the unmanaged C++ code via Platform Invoke (P/Invoke).
///
/// TODO: remove hardcoding of DLL paths.
/// </summary>

namespace Derecho.Cascade
{
    // should match the definition in include/cascade/service.hpp
    enum ShardMemberSelectionPolicy {
        FirstMember,    // use the first member in the list returned from get_shard_members(), this is the default behaviour.
        LastMember,     // use the last member in the list returned from get_shard_members()
        Random,         // use a random member in the shard for each operations(put/remove/get/get_by_time).
        FixedRandom,    // use a random member and stick to that for the following operations.
        RoundRobin,     // use a member in round-robin order.
        KeyHashing,     // use the key's hashing 
        UserSpecified,  // user specify which member to contact.
        InvalidPolicy = -1
    };

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

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct GetArgs
        {
            // Initialize a GetArgs struct with default values.
            public GetArgs()
            {
                subgroupType = "";
                subgroupIndex = UInt32.MaxValue;
                shardIndex = UInt32.MaxValue;
                version = Int64.MinValue;
                stable = true;
                timestamp = 0;
            }
            public string subgroupType;
            public UInt32 subgroupIndex;
            public UInt32 shardIndex;
            public Int64 version;
            public bool stable;
            public UInt64 timestamp;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct PutArgs
        {
            public string subgroupType;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct CsBlob
        {
            public IntPtr bytes; // should be byte*
            public UInt64 size;
        }

        public static string[] LEGAL_CASCADE_SUBGROUP_TYPES = 
        {
            "VolatileCascadeStoreWithStringKey",
            "PersistentCascadeStoreWithStringKey",
            "TriggerCascadeNoStoreWithStringKey"
        };

        /**
         * EXPORTED UNMANAGED UTILITY FUNCTIONS
         */
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern ObjectProperties extractResult(IntPtr queryResultsPtr);

        /**
         * CASCADE SERVICE CLIENT EXPORTED FUNCTIONS
         */
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern UInt32 getSubgroupIndex(IntPtr capi, string serviceType); 

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        private static extern IntPtr get(IntPtr capi, string key, GetArgs args);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static unsafe extern IntPtr put(IntPtr capi, string object_pool_path, CsBlob blob,
            PutArgs args);
        
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern IntPtr getServiceClientRef();

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern UInt32 getMyId(IntPtr capi);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so")]
        public static extern IntPtr createObjectPool(IntPtr capi, string objectPoolPathname, 
            string serviceType, UInt32 subgroupIndex, string affinitySetRegex);

        public static void Main()
        {
            IntPtr capi = getServiceClientRef();
            var nodeId = getMyId(capi);
            var subgroupIndexVcss = getSubgroupIndex(capi, LEGAL_CASCADE_SUBGROUP_TYPES[0]);
            Console.WriteLine("Node id: " + nodeId);
            Console.WriteLine("VCSS subgroup index: " + subgroupIndexVcss);

            createObjectPool(capi, "/console_printer", LEGAL_CASCADE_SUBGROUP_TYPES[0], 0, "");
            Console.WriteLine("Created object pool /console_printer.");

            IntPtr bytesPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            Int32 val = 1;
            Marshal.StructureToPtr(val, bytesPtr, false);        
            PutArgs putArgs = new PutArgs();
            putArgs.subgroupType = LEGAL_CASCADE_SUBGROUP_TYPES[0]; 
            CsBlob blob = new CsBlob();
            blob.size = (UInt64) Marshal.SizeOf(typeof(Int32));
            blob.bytes = bytesPtr;
            put(capi, "/console_printer/obj_a", blob, putArgs);

            Console.WriteLine("Enter key of obj: ");
            string key = Console.ReadLine();

            GetArgs getArgs = new GetArgs();
            getArgs.subgroupType = LEGAL_CASCADE_SUBGROUP_TYPES[0];
            IntPtr queryResult = get(capi, key, getArgs);
            ObjectProperties objResult = extractResult(queryResult);
            Console.WriteLine("Result");
            Console.WriteLine("====================");
            Console.WriteLine("Timestamp: " + objResult.timestamp);
            Console.WriteLine("Version: " + objResult.version);
            Console.WriteLine("Data as string: " + Marshal.PtrToStringAuto(objResult.bytes, (Int32) objResult.bytes_size));
        }
    }
} // namespace Derecho.Cascade
