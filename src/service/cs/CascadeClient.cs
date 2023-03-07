using System;
using System.Text;
using System.Runtime.InteropServices;

namespace Derecho.Cascade
{
    /// <summary>
    /// Shard member selection policies for doing Cascade store operations.
    ///
    /// Should match the definition of the same name in include/cascade/service.hpp.
    /// </summary>
    enum ShardMemberSelectionPolicy
    {
        FirstMember,    // use the first member in the list returned from get_shard_members(), this is the default behaviour.
        LastMember,     // use the last member in the list returned from get_shard_members()
        Random,         // use a random member in the shard for each operations(put/remove/get/get_by_time).
        FixedRandom,    // use a random member and stick to that for the following operations.
        RoundRobin,     // use a member in round-robin order.
        KeyHashing,     // use the key's hashing 
        UserSpecified,  // user specify which member to contact.
        InvalidPolicy = -1
    };

    /// <summary>
    /// This class contains instance methods for accessing the Cascade external service client API.
    /// These functions are exposed from the unmanaged C++ code via Platform Invoke (P/Invoke),
    /// but are made private to wrap with more usable C# logic.
    /// </summary>
    ///
    /// <example>
    /// To use the client, create an instance of <c>CascadeClient</c>. This
    /// will setup the service client reference internally. Then, you may use
    /// its class methods to do operations on the Cascade store as required.
    /// </example>
    public unsafe class CascadeClient
    {
        private IntPtr capi;
        
        /// <summary>
        /// The constructor for <c>CascadeClient</c>. Initializes a
        /// external service client reference for Cascade, so we can
        /// use this instance for all the client operations in C#.
        /// </summary>
        public CascadeClient()
        {
            capi = EXPORT_getServiceClientRef();
        }

        public enum SubgroupType
        {
            VolatileCascadeStoreWithStringKey,
            PersistentCascadeStoreWithStringKey,
            TriggerCascadeNoStoreWithStringKey
        }

        // Structs and Definitions
        [StructLayout(LayoutKind.Sequential)]
        public struct ObjectProperties
        {
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
        public struct StdVectorWrapper
        {
            IntPtr data;
            UInt64 length;
        }

        private static string[] LEGAL_CASCADE_SUBGROUP_TYPES = 
        {
            "VolatileCascadeStoreWithStringKey",
            "PersistentCascadeStoreWithStringKey",
            "TriggerCascadeNoStoreWithStringKey"
        };

        /**
         * Exported Utility Functions (from unmanaged code)
         * These do not depend on a service client API reference, so
         * we do not redefine them in C#.
         */
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern ObjectProperties extractObjectPropertiesFromQueryResults(IntPtr queryResultsPtr);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern void freePointer(IntPtr ptr);

        /**
         * Cascade C# Service Client Exported Functions (from unmanaged code)
         */
        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_getServiceClientRef();

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getMyId(IntPtr capi);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getSubgroupIndex(IntPtr capi, string serviceType); 

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_get(IntPtr capi, string key, GetArgs args);

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static unsafe extern IntPtr EXPORT_put(IntPtr capi, string object_pool_path, byte[] bytes, UInt64 bytesSize, PutArgs args);      

        [DllImport("/root/workspace/cascade/build-Release/src/service/cs/libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_createObjectPool(IntPtr capi, string objectPoolPathname, 
            string serviceType, UInt32 subgroupIndex, string affinitySetRegex);

        /************************
         * Class Implementations
         ************************/

        private static string subgroupEnumToString(SubgroupType? type)
        {
            if (type is null)
            {
                return "";
            }

            switch (type)
            {
                case SubgroupType.VolatileCascadeStoreWithStringKey:
                    return LEGAL_CASCADE_SUBGROUP_TYPES[0];
                case SubgroupType.PersistentCascadeStoreWithStringKey:
                    return LEGAL_CASCADE_SUBGROUP_TYPES[1];
                case SubgroupType.TriggerCascadeNoStoreWithStringKey:
                    return LEGAL_CASCADE_SUBGROUP_TYPES[2];
                default:
                    throw new Exception("Impossible");
            }
        }

        /// <summary>
        /// Gets the client node's ID.
        /// </summary>
        public UInt32 GetMyId()
        {
            return EXPORT_getMyId(capi);
        }

        /// <summary>
        /// Gets the client node's subgroup index for a given
        /// service type.
        /// </summary>
        /// <param><c>type</c> is the subgroup type for the index we want</param>
        /// <returns>The subgroup index.</returns>
        public UInt32 GetSubgroupIndex(SubgroupType type)
        {
            return EXPORT_getSubgroupIndex(capi, subgroupEnumToString(type));
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct GetArgs
        {
            // Initialize a GetArgs struct with default values.
            public GetArgs(SubgroupType? subgroupType, UInt32 subgroupIndex, UInt32 shardIndex, 
                Int64 version, bool stable, UInt64 timestamp)
            {
                this.subgroupType = subgroupEnumToString(subgroupType);
                this.subgroupIndex = subgroupIndex;
                this.shardIndex = shardIndex;
                this.version = version;
                this.stable = stable;
                this.timestamp = timestamp;
            }
            public string subgroupType;
            public UInt32 subgroupIndex;
            public UInt32 shardIndex;
            public Int64 version;
            public bool stable;
            public UInt64 timestamp;
        }

        /// <summary>
        /// Get an object from Cascade's store.
        /// </summary>
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <param><c>version</c> is the version to specify for a versioned get. 
        ///                       Defaults to the current version.
        /// </param>
        /// <param><c>stable</c> if getting stable data. Defaults to true.</param>
        /// <param><c>timestamp</c> is the Unix epoch ms for a timestamped get. Defaults to
        ///                         not using a timestamp get.
        /// </param>
        /// <returns>An ObjectProperties struct of the data associated with the object.</returns>
        public ObjectProperties Get(string key, 
                                    SubgroupType? type = null, 
                                    UInt32 subgroupIndex = 0, 
                                    UInt32 shardIndex = 0,
                                    // this means current version
                                    Int64 version = -1L,
                                    bool stable = true,
                                    UInt64 timestamp = 0)
        {
            GetArgs args = new GetArgs(type, subgroupIndex, shardIndex, version, stable, timestamp);
            IntPtr getResult = EXPORT_get(capi, key, args);
            return extractObjectPropertiesFromQueryResults(getResult);
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PutArgs
        {
            public PutArgs(string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex, 
                Int64 previousVersion, Int64 previousVersionByKey, bool blocking, bool trigger,
                UInt64 messageId)
            {
                this.subgroupType = subgroupType; 
                this.subgroupIndex = subgroupIndex;
                this.shardIndex = shardIndex;
                this.previousVersion = previousVersion;
                this.previousVersionByKey = previousVersionByKey;
                this.blocking = blocking;
                this.trigger = trigger;
                this.messageId = messageId;
            }

            public string subgroupType;
            public UInt32 subgroupIndex;
            public UInt32 shardIndex;
            public Int64 previousVersion;
            public Int64 previousVersionByKey;
            public bool blocking;
            public bool trigger;
            public UInt64 messageId;

        }

        /// <summary>
        /// Put an object into Cascade's store.
        /// </summary>
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>bytes</c> are the blob bytes of the data in a byte array.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from.
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to the max subgroup index.</param>
        /// <param><c>shardIndex</c> Defaults to zero.</param>
        /// <param><c>previousVersion</c> Defaults to the current version.</param>
        /// <param><c>previousVersionByKey</c> Defaults to the current version.</param>
        /// <param><c>blocking</c> Defaults to true.</param>
        /// <param><c>trigger</c> Defaults to false.</param>
        /// <param><c>messageId</c></param>
        public IntPtr Put(string key, 
                          byte[] bytes,
                          SubgroupType? type = null,
                          UInt32 subgroupIndex = UInt32.MaxValue,
                          UInt32 shardIndex = 0,
                          Int64 previousVersion = -1L,
                          Int64 previousVersionByKey = -1L,
                          bool blocking = true,
                          bool trigger = false,
                          UInt64 messageId = 0)
        {
            PutArgs args = new PutArgs(subgroupEnumToString(type), subgroupIndex,
                shardIndex, previousVersion, previousVersionByKey, blocking, trigger, messageId);
            return EXPORT_put(capi, key, bytes, (UInt64) bytes.Length, args);
        }

        /// <summary>
        /// </summary>
        public IntPtr CreateObjectPool(string objectPoolPathname, SubgroupType type, 
                                       UInt32 subgroupIndex, string affinitySetRegex = "")
        {
            return EXPORT_createObjectPool(capi, objectPoolPathname, subgroupEnumToString(type), 
                subgroupIndex, affinitySetRegex);
        }

        public unsafe static void Main()
        {
            CascadeClient client = new CascadeClient();
            var nodeId = client.GetMyId();
            var subgroupIndexVcss = client.GetSubgroupIndex(SubgroupType.VolatileCascadeStoreWithStringKey);
            Console.WriteLine("Node id: " + nodeId);
            Console.WriteLine("VCSS subgroup index: " + subgroupIndexVcss);

            client.CreateObjectPool("/console_printer", SubgroupType.VolatileCascadeStoreWithStringKey, 0, "");
            Console.WriteLine("Created object pool /console_printer.");

            byte[] byteArray = Encoding.UTF8.GetBytes("hello");
            for (int i = 0; i < byteArray.Length; i++)
            {
                Console.WriteLine(byteArray[i].ToString());
            }

            client.Put("/console_printer/obj_a", byteArray);

            // string key = "/console_printer/obj_a";


            // IntPtr queryResult = get(capi, key, getArgs);
            // ObjectProperties objResult = extractResult(queryResult);

            // Console.WriteLine("Result");
            // Console.WriteLine("====================");
            // Console.WriteLine("Object byte ptr: " + objResult.bytes);
            // Console.WriteLine("Timestamp: " + objResult.timestamp);
            // Console.WriteLine("Version: " + objResult.version);
            // Console.WriteLine("Bytes size: " + objResult.bytes_size);

            // // char[] data = new char[objResult.bytes_size];
            // // byte* objectBytePointer = (byte*) objResult.bytes;
            // // for (UInt64 i = 0; i < objResult.bytes_size; i++)
            // // {
            // //     char c = (char) *objectBytePointer;
            // //     data[i] = c;
            // //     ++objectBytePointer;
            // // }
            // // string result = new string(data);
            // Console.WriteLine("Data as string: " + Marshal.PtrToStringUTF8(objResult.bytes));
            // freePointer(objResult.bytes);
        }
    }
} // namespace Derecho.Cascade
