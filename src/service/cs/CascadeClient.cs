using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Linq;

namespace Derecho.Cascade
{
    /// <summary>
    /// Shard member selection policies for doing Cascade store operations.
    ///
    /// Should match the definition of the same name in include/cascade/service.hpp.
    /// </summary>
    public enum ShardMemberSelectionPolicy
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
        public const Int64 CURRENT_VERSION = -1L;

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
            private IntPtr key;
            // needs to be freed after use.
            private IntPtr bytes;
            public UInt64 bytesSize;
            public Int64 version;
            public UInt64 timestamp;
            public Int64 previousVersion;
            public Int64 previousVersionByKey;
            public UInt64 messageId;

            public string GetKey()
            {
                return Marshal.PtrToStringAuto(key);
            }

            public byte* GetBytePtr()
            {
                return (byte*) bytes.ToPointer();
            }

            public string BytesToString()
            {
                return Marshal.PtrToStringAuto(bytes);
            }

            public override string ToString()
            {
                return $@"ObjectProperties{{bytes:{BytesToString()}, bytesSize:{bytesSize},
                    version:{version}, timestamp:{timestamp}, previousVersion:{previousVersion},
                    previousVersionByKey:{previousVersionByKey}, messageId:{messageId}
                }}";
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct VersionTimestampPair
        {
            public Int64 version;
            public UInt64 timestamp;

            public override string ToString()
            {
                return $"VersionTimestampPair{{version:{version}, timestamp:{timestamp}}}";
            }
        }

        public struct PolicyMetadata
        {
            public string policyString;
            public ShardMemberSelectionPolicy policy;
            public UInt32 userNode;

            public override string ToString()
            {
                return $@"PolicyMetadata{{policyString:{policyString}, policy:{policy}, userNode:{userNode}}}";
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PolicyMetadataInternal
        {
            public IntPtr policyString;
            public ShardMemberSelectionPolicy policy;
            public UInt32 userNode;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct StdVectorWrapper
        {
            public IntPtr data;
            public IntPtr vecBasePtr;
            public UInt64 length;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct TwoDimensionalNodeList
        {
            public StdVectorWrapper flattenedList;
            public StdVectorWrapper vectorSizes;
        }
        
        public struct GetObjectPoolMetadata
        {
            public Int64 version;
            public UInt64 timestamp;
            public Int64 previousVersion;
            public Int64 previousVersionByKey;
            public string pathname;
            public UInt32 subgroupTypeIndex;
            public UInt32 subgroupIndex;
            public Int32 shardingPolicy;
            public Dictionary<string, UInt32> objectLocations;
            public string affinitySetRegex;
            public bool deleted;

            public override string ToString()
            {
                return $@"GetObjectPoolMetadata{{version:{version}, timestamp:{timestamp}, 
                    previousVersion:{previousVersion}, previousVersionByKey:{previousVersionByKey},
                    pathname:{pathname}, subgroupTypeIndex:{subgroupTypeIndex}, subgroupIndex:{subgroupIndex},
                    shardingPolicy:{shardingPolicy}, objectLocations:{objectLocationsToString(objectLocations)},
                    affinitySetRegex:{affinitySetRegex}, deleted:{deleted}
                }}"; 
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct GetObjectPoolMetadataInternal
        {
            public Int64 version;
            public UInt64 timestamp;
            public Int64 previousVersion;
            public Int64 previousVersionByKey;
            // string
            public IntPtr pathname;
            public UInt32 subgroupTypeIndex;
            public UInt32 subgroupIndex;
            public Int32 shardingPolicy;
            public StdVectorWrapper objectLocations;
            // string
            public IntPtr affinitySetRegex;
            public bool deleted;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ObjectLocation
        {
            public IntPtr key;
            public UInt32 shard;
        }

        private static string[] LEGAL_CASCADE_SUBGROUP_TYPES = 
        {
            "VolatileCascadeStoreWithStringKey",
            "PersistentCascadeStoreWithStringKey",
            "TriggerCascadeNoStoreWithStringKey"
        };

#if IS_EXTERNAL_CLIENT
    public const string CLIENT_DLL = "libexternal_client_cs.so";
#else
    public const string CLIENT_DLL = "libmember_client_cs.so";
#endif

        /**
         * Exported Utility Functions (from unmanaged code)
         * These do not depend on a service client API reference, so
         * we do not redefine them in C#.
         */

        // Private helpers
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper indexTwoDimensionalNodeVector(IntPtr vec, UInt64 index);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern ObjectProperties extractObjectPropertiesFromQueryResults(IntPtr queryResultsPtr);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern VersionTimestampPair extractVersionTimestampFromQueryResults(IntPtr queryResultsPtr);
        
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt64 extractUInt64FromQueryResults(IntPtr queryResults);
        
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper extractStdVectorWrapperFromQueryResults(IntPtr queryResults);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr indexStdVectorWrapperString(StdVectorWrapper vector, UInt64 index);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern ObjectLocation indexStdVectorWrapperObjectLocation(StdVectorWrapper vector, UInt64 index);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr indexStdVectorWrapperStringVectorQueryResults(StdVectorWrapper vector, UInt64 index);

        // Public utilities
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool deleteObjectLocationVectorPointer(IntPtr ptr);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool deleteStringVectorPointer(IntPtr ptr);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool deleteNodeIdVectorPointer(IntPtr ptr);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool freeBytePointer(IntPtr ptr);

        /**
         * Cascade C# Service Client Exported Functions (from unmanaged code)
         */
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_getServiceClientRef();

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getMyId(IntPtr capi);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getSubgroupIndex(IntPtr capi, string serviceType); 

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_get(IntPtr capi, string key, GetArgs args);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static unsafe extern IntPtr EXPORT_put(IntPtr capi, string object_pool_path, byte[] bytes, UInt64 bytesSize, PutArgs args);      

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getNumberOfSubgroups(IntPtr capi, string serviceType);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getNumberOfShards(IntPtr capi, string serviceType, UInt32 shardIndex);
    
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getMembers(IntPtr capi);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern TwoDimensionalNodeList EXPORT_getSubgroupMembers(IntPtr capi, string serviceType, UInt32 subgroupIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern TwoDimensionalNodeList EXPORT_getSubgroupMembersByObjectPool(IntPtr capi, string objectPoolPathname);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getShardMembers(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getShardMembersByObjectPool(IntPtr capi, string objectPoolPathname, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern void EXPORT_setMemberSelectionPolicy(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex, string policy, UInt32 userNode);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern PolicyMetadataInternal EXPORT_getMemberSelectionPolicy(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_remove(IntPtr capi, string key, string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_multiGet(IntPtr capi, string key, string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_getSize(IntPtr capi, string key, string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex, Int64 version, bool stable, UInt64 timestamp);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_multiGetSize(IntPtr capi, string key, string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_listKeysInShard(IntPtr capi, string subgroupType, UInt32 subgroupIndex, UInt32 shardIndex, Int64 version, bool stable, UInt64 timestamp);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_listKeysInObjectPool(IntPtr capi, string objectPoolPathname, Int64 version, bool stable, UInt64 timestamp);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_multiListKeysInShard(IntPtr capi, string subgroupType, UInt32 subgroupindex, UInt32 shardIndex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_multiListKeysInObjectPool(IntPtr capi, string objectPoolPathname);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_listObjectPools(IntPtr capi);
        
        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_createObjectPool(IntPtr capi, string objectPoolPathname, 
            string serviceType, UInt32 subgroupIndex, string affinitySetRegex);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern GetObjectPoolMetadataInternal EXPORT_getObjectPool(IntPtr capi, string objectPoolPathname);

        [DllImport(CLIENT_DLL, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_removeObjectPool(IntPtr capi, string objectPoolPathname);

        /************************
         * Class helpers
         ************************/
        private static string objectLocationsToString(Dictionary<string, UInt32> dictionary)
        {
            var lines = dictionary.Select(kvp => kvp.Key + ": " + kvp.Value.ToString());
            return "[" + string.Join(", ", lines) + "]";
        }

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
                    throw new ArgumentException("Impossible subgroup type enum.");
            }
        }

        private static string shardMemberSelectionPolicyToString(ShardMemberSelectionPolicy policy)
        {
            switch (policy)
            {
                case ShardMemberSelectionPolicy.FirstMember:
                    return "FirstMember";
                case ShardMemberSelectionPolicy.LastMember:
                    return "LastMember";
                case ShardMemberSelectionPolicy.Random:
                    return "Random";
                case ShardMemberSelectionPolicy.FixedRandom:
                    return "FixedRandom";
                case ShardMemberSelectionPolicy.RoundRobin:
                    return "RoundRobin";
                case ShardMemberSelectionPolicy.KeyHashing:
                    return "KeyHashing";
                case ShardMemberSelectionPolicy.UserSpecified:
                    return "UserSpecified";
                case ShardMemberSelectionPolicy.InvalidPolicy:
                    return "InvalidPolicy";
                default:
                    throw new ArgumentException("Impossible policy enum.");
            }
        }

        private unsafe static List<UInt32> extractNodeList(StdVectorWrapper vector)
        {
            List<UInt32> members = new List<UInt32>();
            UInt32* ptr = (UInt32*) vector.data;
            for (UInt64 i = 0; i < vector.length; i++)
            {
                members.Add(*ptr);
                ++ptr;
            }

            deleteNodeIdVectorPointer(vector.vecBasePtr);
            return members;
        }

        private unsafe static List<List<UInt32>> extract2DNodeList(TwoDimensionalNodeList vector)
        {
            List<List<UInt32>> outerList = new List<List<UInt32>>();
            StdVectorWrapper flattenedList = vector.flattenedList;
            StdVectorWrapper vectorSizes = vector.vectorSizes;

            UInt32* nodePtr = (UInt32*) flattenedList.data;
            UInt64* sizePtr = (UInt64*) vectorSizes.data;    
            for (UInt64 i = 0; i < vectorSizes.length; i++)
            {
                UInt64 size = *sizePtr;
                List<UInt32> innerList = new List<UInt32>();
                for (UInt64 j = 0; j < size; j++)
                {
                    innerList.Add(*nodePtr);
                    ++nodePtr;
                }
                outerList.Add(innerList);
                ++sizePtr;
            }
            
            deleteNodeIdVectorPointer(flattenedList.vecBasePtr);
            deleteNodeIdVectorPointer(vectorSizes.vecBasePtr);
            return outerList;
        }

        private unsafe static List<string> extractStringListFromStdVector(StdVectorWrapper vector)
        {
            List<string> list = new List<string>();
            for (UInt64 i = 0; i < vector.length; i++)
            {
                IntPtr strPtr = indexStdVectorWrapperString(vector, i);
                list.Add(Marshal.PtrToStringAuto(strPtr));
            }
            return list;
        }

        private unsafe static GetObjectPoolMetadata extractObjectPoolMetadata(GetObjectPoolMetadataInternal metadataInternal)
        {
            // set same fields
            GetObjectPoolMetadata metadata = new GetObjectPoolMetadata();
            metadata.version = metadataInternal.version;
            metadata.timestamp = metadataInternal.timestamp;
            metadata.previousVersion = metadataInternal.previousVersion;
            metadata.previousVersionByKey = metadataInternal.previousVersionByKey;
            metadata.subgroupTypeIndex = metadataInternal.subgroupTypeIndex;
            metadata.subgroupIndex = metadataInternal.subgroupIndex;
            metadata.shardingPolicy = metadataInternal.shardingPolicy;
            metadata.deleted = metadataInternal.deleted;
            
            // marshal non-blittable types
            metadata.pathname = Marshal.PtrToStringAuto(metadataInternal.pathname);
            metadata.affinitySetRegex = Marshal.PtrToStringAuto(metadataInternal.affinitySetRegex);
            Dictionary<string, UInt32> objectLocations = new Dictionary<string, UInt32>();
            StdVectorWrapper vector = metadataInternal.objectLocations;
            for (UInt64 i = 0; i < vector.length; i++)
            {
                ObjectLocation objectLocation = indexStdVectorWrapperObjectLocation(vector, i);
                objectLocations.Add(Marshal.PtrToStringAuto(objectLocation.key), objectLocation.shard);
            }
            metadata.objectLocations = objectLocations;
            deleteObjectLocationVectorPointer(vector.vecBasePtr);
            
            return metadata;
        }

        /**************************************
         * Client class functions
         **************************************/
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
        /// </summary>
        public ObjectProperties Get(string key, 
                                    SubgroupType? type = null, 
                                    UInt32 subgroupIndex = 0, 
                                    UInt32 shardIndex = 0,
                                    // this means current version
                                    Int64 version = CURRENT_VERSION,
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
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>bytes</c> are the blob bytes of the data in a byte array.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from.
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to zero.</param>
        /// <param><c>shardIndex</c> Defaults to zero.</param>
        /// <param><c>previousVersion</c> Defaults to the current version.</param>
        /// <param><c>previousVersionByKey</c> Defaults to the current version.</param>
        /// <param><c>blocking</c> Defaults to true.</param>
        /// <param><c>trigger</c> Defaults to false.</param>
        /// <param><c>messageId</c></param>
        /// <returns>A VersionTimestampPair response.</returns>
        /// </summary>
        public VersionTimestampPair Put(string key, 
                          byte[] bytes,
                          SubgroupType? type = null,
                          UInt32 subgroupIndex = 0,
                          UInt32 shardIndex = 0,
                          Int64 previousVersion = CURRENT_VERSION,
                          Int64 previousVersionByKey = CURRENT_VERSION,
                          bool blocking = true,
                          bool trigger = false,
                          UInt64 messageId = 0)
        {
            PutArgs args = new PutArgs(subgroupEnumToString(type), subgroupIndex,
                shardIndex, previousVersion, previousVersionByKey, blocking, trigger, messageId);
            return extractVersionTimestampFromQueryResults(
                EXPORT_put(capi, key, bytes, (UInt64) bytes.Length, args));
        }

        /// <summary>
        /// Put a string value into Cascade's store.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>str</c> is the string to put into Cascade via bytes.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from.
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to zero.</param>
        /// <param><c>shardIndex</c> Defaults to zero.</param>
        /// <param><c>previousVersion</c> Defaults to the current version.</param>
        /// <param><c>previousVersionByKey</c> Defaults to the current version.</param>
        /// <param><c>blocking</c> Defaults to true.</param>
        /// <param><c>trigger</c> Defaults to false.</param>
        /// <param><c>messageId</c></param>
        /// <returns>A VersionTimestampPair response.</returns>
        /// </summary>
        public VersionTimestampPair Put(string key, 
                          string str,
                          SubgroupType? type = null,
                          UInt32 subgroupIndex = 0,
                          UInt32 shardIndex = 0,
                          Int64 previousVersion = CURRENT_VERSION,
                          Int64 previousVersionByKey = CURRENT_VERSION,
                          bool blocking = true,
                          bool trigger = false,
                          UInt64 messageId = 0)
        {

            PutArgs args = new PutArgs(subgroupEnumToString(type), subgroupIndex,
                shardIndex, previousVersion, previousVersionByKey, blocking, trigger, messageId);
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(str);
            return extractVersionTimestampFromQueryResults(
                EXPORT_put(capi, key, bytes, (UInt64) bytes.Length, args));
        }

        /// <summary>
        /// Remove an object from Cascade's store.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from.
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to zero.</param>
        /// <param><c>shardIndex</c> Defaults to zero.</param>
        /// <returns>A VersionTimestampPair response.</returns>
        /// </summary>
        public VersionTimestampPair Remove(string key, 
                                           SubgroupType? type = null, 
                                           UInt32 subgroupIndex = 0, 
                                           UInt32 shardIndex = 0)
        {
            IntPtr res = EXPORT_remove(capi, key, subgroupEnumToString(type), subgroupIndex, 
                shardIndex);
            return extractVersionTimestampFromQueryResults(res);
        }
        
        /// <summary>
        /// Get an object from Cascade's store using multi_get.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <returns>An ObjectProperties struct of the data associated with the object.</returns>
        /// </summary>
        public ObjectProperties MultiGet(string key,
                                         SubgroupType? type = null,
                                         UInt32 subgroupIndex = 0,
                                         UInt32 shardIndex = 0)
        {
            IntPtr res = EXPORT_multiGet(capi, key, subgroupEnumToString(type), subgroupIndex,
                shardIndex);
            return extractObjectPropertiesFromQueryResults(res);
        }       

        /// <summary>
        /// Get the size of an object using get_size.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <param><c>version</c> is the version to specify for a versioned get size. 
        ///                       Defaults to the current version.
        /// </param>
        /// <param><c>stable</c> if getting stable data. Defaults to true.</param>
        /// <param><c>timestamp</c> is the Unix epoch ms for a timestamped get. Defaults to
        ///                         not using a timestamp get.
        /// </param>
        /// <returns>The size of the object.</returns>
        /// </summary>
        public UInt64 GetSize(string key,
                              SubgroupType? type = null,
                              UInt32 subgroupIndex = 0,
                              UInt32 shardIndex = 0,
                              Int64 version = CURRENT_VERSION,
                              bool stable = true,
                              UInt64 timestamp = 0L)
        {
            IntPtr res = EXPORT_getSize(capi, key, subgroupEnumToString(type), subgroupIndex, 
                shardIndex, version, stable, timestamp);
            return extractUInt64FromQueryResults(res);
        }


        /// <summary>
        /// Get the size of an object using multi_get_size.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        ///                    Defaults to none.
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <returns>The size of the object.</returns>
        /// </summary>
        public UInt64 MultiGetSize(string key,
                              SubgroupType? type = null,
                              UInt32 subgroupIndex = 0,
                              UInt32 shardIndex = 0)
        {
            IntPtr res = EXPORT_multiGetSize(capi, key, subgroupEnumToString(type), subgroupIndex, 
                shardIndex);
            return extractUInt64FromQueryResults(res);
        }

        /// <summary>
        /// List the keys in a given shard.
        /// <param><c>key</c> is the key of the object.</param>
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <param><c>version</c> is the version to specify for a versioned get size. 
        ///                       Defaults to the current version.
        /// </param>
        /// <param><c>stable</c> if getting stable data. Defaults to true.</param>
        /// <param><c>timestamp</c> is the Unix epoch ms for a timestamped get. Defaults to
        ///                         not using a timestamp get.
        /// </param>
        /// <returns>The keys in the shard.</returns>
        /// </summary>
        public List<string> ListKeysInShard(SubgroupType type,
                              UInt32 subgroupIndex = 0,
                              UInt32 shardIndex = 0,
                              Int64 version = CURRENT_VERSION,
                              bool stable = true,
                              UInt64 timestamp = 0L)
        {
            IntPtr res = EXPORT_listKeysInShard(capi, subgroupEnumToString(type), subgroupIndex, 
                shardIndex, version, stable, timestamp);
            StdVectorWrapper vector = extractStdVectorWrapperFromQueryResults(res);
            return extractStringListFromStdVector(vector);
        }

        /// <summary>
        /// List the keys in a given object pool.
        /// <param><c>objectPoolPathname</c></param>
        /// <param><c>version</c> is the version to specify for a versioned get size. 
        ///                       Defaults to the current version.
        /// </param>
        /// <param><c>stable</c> if getting stable data. Defaults to true.</param>
        /// <param><c>timestamp</c> is the Unix epoch ms for a timestamped get. Defaults to
        ///                         not using a timestamp get.
        /// </param>
        /// <returns>The keys in the object pool.</returns>
        /// </summary>
        public List<string> ListKeysInObjectPool(string objectPoolPathname,
                                                 Int64 version = CURRENT_VERSION,
                                                 bool stable = true,
                                                 UInt64 timestamp = 0L)
        {
            List<string> keys = new List<string>();
            StdVectorWrapper res = EXPORT_listKeysInObjectPool(capi, objectPoolPathname, version, stable,
                timestamp);
            for (UInt64 i = 0; i < res.length; i++)
            {
                var queryResults = indexStdVectorWrapperStringVectorQueryResults(res, i);
                var queryResultsVec = extractStdVectorWrapperFromQueryResults(queryResults);
                var queryResultsList = extractStringListFromStdVector(queryResultsVec);
                foreach (var str in queryResultsList)
                {
                    keys.Add(str);
                } 
            }
            return keys;
        }

        /// <summary>
        /// List the keys in a shard using multi_get.
        /// <param><c>type</c> is the subgroup type in Cascade to get from. 
        /// </param>
        /// <param><c>subgroupIndex</c> Defaults to 0.</param>
        /// <param><c>shardIndex</c> Defaults to 0.</param>
        /// <returns>The keys in the shard.</returns>
        /// </summary>
        public List<string> MultiListKeysInShard(SubgroupType type, 
                                                 UInt32 subgroupIndex = 0,
                                                 UInt32 shardIndex = 0)
        {
            IntPtr res = EXPORT_multiListKeysInShard(capi, subgroupEnumToString(type), 
                subgroupIndex, shardIndex);
            StdVectorWrapper vector = extractStdVectorWrapperFromQueryResults(res);
            return extractStringListFromStdVector(vector);
        }

        /// <summary>
        /// List the keys in an object pool using multi_get.
        /// <param><c>objectPoolPathname</c></param>
        /// <returns>The keys in the object pool.</returns>
        /// </summary>
        public List<string> MultiListKeysInObjectPool(string objectPoolPathname)
        {
            List<string> keys = new List<string>();
            StdVectorWrapper res = EXPORT_multiListKeysInObjectPool(capi, objectPoolPathname);
            for (UInt64 i = 0; i < res.length; i++)
            {
                var queryResults = indexStdVectorWrapperStringVectorQueryResults(res, i);
                var queryResultsVec = extractStdVectorWrapperFromQueryResults(queryResults);
                var queryResultsList = extractStringListFromStdVector(queryResultsVec);
                keys.Concat(queryResultsList);
            }
            return keys;
        }

        /// <summary>
        /// List all the object pools.
        /// <returns>The keys in the object pool.</returns>
        /// </summary>
        public List<string> ListObjectPools()
        {
            StdVectorWrapper vector = EXPORT_listObjectPools(capi);
            List<string> list = extractStringListFromStdVector(vector);
            // We need to delete the pointer here, since it is not contained within a QueryResults object
            // so it will never actually get destructed.
            deleteStringVectorPointer(vector.vecBasePtr);
            return list;
        }

        /// <summary>
        /// Create an object pool.
        /// <param><c>type</c> is the subgroup type.</param>
        /// <param><c>subgroupIndex</c></param>
        /// <param><c>affinitySetRegex</c> defaults to an empty string.</param>
        /// <returns>A version and timestamp pair response.</returns>
        /// </summary>
        public VersionTimestampPair CreateObjectPool(string objectPoolPathname, SubgroupType type, 
                                       UInt32 subgroupIndex, string affinitySetRegex = "")
        {
            return extractVersionTimestampFromQueryResults(
                EXPORT_createObjectPool(capi, objectPoolPathname, subgroupEnumToString(type), 
                    subgroupIndex, affinitySetRegex));
        }

        /// <summary>
        /// Remove an object pool.
        /// <param><c>objectPoolPathname</c></param>
        /// <returns>A version and timestamp pair response.</returns>
        /// </summary>
        public VersionTimestampPair RemoveObjectPool(string objectPoolPathname)
        {
            IntPtr res = EXPORT_removeObjectPool(capi, objectPoolPathname);
            return extractVersionTimestampFromQueryResults(res);
        }

        /// <summary>
        /// Get the metadata associated with an object pool.
        /// <param><c>objectPoolPathname</c></param>
        ///
        /// <returns>A GetObjectPoolMetadata response.</returns>
        /// </summary>
        public GetObjectPoolMetadata GetObjectPool(string objectPoolPathname)
        {
            GetObjectPoolMetadataInternal res = EXPORT_getObjectPool(capi, objectPoolPathname);
            return extractObjectPoolMetadata(res);
        }
        
        /// <summary>
        /// Get the total number of subgroups.
        ///
        /// <param><c>type</c> is the subgroup type.</param>
            /// <returns>The number of subgroups under the service type.</returns>
        /// </summary>
        public UInt32 GetNumberOfSubgroups(SubgroupType type)
        {
            return EXPORT_getNumberOfSubgroups(capi, subgroupEnumToString(type));
        }

        /// <summary>
        /// Get the total number of shards within a subgroup.
        ///
        /// <param><c>type</c> is the subgroup type.</param>
        /// <param><c>subgroupIndex</c></param>
        /// <returns>The number of shards.</returns>
        /// </summary>
        public UInt32 GetNumberOfShards(SubgroupType type, UInt32 subgroupIndex)
        {
            return EXPORT_getNumberOfShards(capi, subgroupEnumToString(type), subgroupIndex);
        }

        /// <summary>
        /// Get the members in the current Derecho group as a list of node IDs.
        /// <returns>A list of node IDs of the members.</returns>
        /// </summary>
        public List<UInt32> GetMembers()
        {  
            StdVectorWrapper vector = EXPORT_getMembers(capi);
            return extractNodeList(vector);
        }

        /// <summary>
        /// Get the members of a shard.
        /// <param><c>type</c> is the service type. </param>
        /// <param><c>subgroupIndex</c></param>
        /// <param><c>shardIndex</c></param>
        /// <returns>A list of node IDs of the members.</returns>
        /// </summary>
        public List<UInt32> GetShardMembers(SubgroupType type, UInt32 subgroupIndex, UInt32 shardIndex)
        {
            StdVectorWrapper vector = EXPORT_getShardMembers(capi, subgroupEnumToString(type), 
                subgroupIndex, shardIndex);
            return extractNodeList(vector);
        }

        /// <summary>
        /// Get the members of a shard, by its object pool path.
        /// <param><c>objectPoolPathname</c></param>
        /// <param><c>shardIndex</c></param>
        /// <returns>A list of node IDs of the members.</returns>
        /// </summary>
        public List<UInt32> GetShardMembersByObjectPool(string objectPoolPathname,
                                                        UInt32 shardIndex)
        {
            StdVectorWrapper vector = EXPORT_getShardMembersByObjectPool(capi, objectPoolPathname,
                shardIndex);
            return extractNodeList(vector);
        }

        /// <summary>
        /// Get members of a subgroup.
        /// <param><c>type</c> is the subgroup type.</param>
        /// <param><c>subgroupIndex</c></param>
        /// </summary>
        public List<List<UInt32>> GetSubgroupMembers(SubgroupType type, UInt32 subgroupIndex)
        {
            TwoDimensionalNodeList vector = EXPORT_getSubgroupMembers(capi, 
                subgroupEnumToString(type), subgroupIndex);
            List<List<UInt32>> subgroupMembers = extract2DNodeList(vector);
            return subgroupMembers;
        }

        /// <summary>
        /// Get members of a subgroup based on an object pool path name.
        /// <param><c>objectPoolPathname</c></param>
        /// </summary>
        public List<List<UInt32>> GetSubgroupMembersByObjectPool(string objectPoolPathname)
        {
            TwoDimensionalNodeList vector = EXPORT_getSubgroupMembersByObjectPool(capi, 
                objectPoolPathname);
            List<List<UInt32>> subgroupMembers = extract2DNodeList(vector);
            return subgroupMembers;
        }

        /// <summary>
        /// Set the member selection policy for a given subgroup and shard index.
        ///
        /// <param><c>type</c> is the subgroup type.</param>
        /// <param><c>subgroupIndex</c>
        /// <param><c>shardIndex</c>
        /// <param><c>policy</c>
        /// <param><c>userNode</c>
        /// </summary>
        public void SetMemberSelectionPolicy(SubgroupType type, 
                                             UInt32 subgroupIndex, 
                                             UInt32 shardIndex, 
                                             ShardMemberSelectionPolicy policy, 
                                             UInt32 userNode)
        {
            EXPORT_setMemberSelectionPolicy(capi, subgroupEnumToString(type), subgroupIndex, 
                shardIndex, shardMemberSelectionPolicyToString(policy), userNode);
        }

        /// <summary>
        /// Get the member selection policy for a given subgroup and shard index.
        ///
        /// <param><c>type</c> is the subgroup type.</param>
        /// <param><c>subgroupIndex</c>
        /// <param><c>shardIndex</c>
        /// <param><c>policy</c>
        /// <param><c>userNode</c>
        /// </summary>
        public PolicyMetadata GetMemberSelectionPolicy(SubgroupType type, 
                                                       UInt32 subgroupIndex, 
                                                       UInt32 shardIndex)
        {
            PolicyMetadataInternal metadataInternal = EXPORT_getMemberSelectionPolicy(capi, 
                subgroupEnumToString(type), subgroupIndex, shardIndex);
            PolicyMetadata metadata = new PolicyMetadata();
            metadata.policyString = shardMemberSelectionPolicyToString(metadataInternal.policy);
            metadata.policy = metadataInternal.policy;
            metadata.userNode = metadataInternal.userNode;
            return metadata;
        }

        /// <summary>
        /// Get all objects within a shard for use with LINQ.
        /// </summary>
        public IEnumerable<ObjectProperties> FromShard(SubgroupType type,
                                                      UInt32 subgroupIndex,
                                                      UInt32 shardIndex,
                                                      Int64 version = CURRENT_VERSION)
        {
            List<string> keysInShard = ListKeysInShard(type, subgroupIndex, shardIndex, stable: true);
            foreach (string key in keysInShard)
            {
                yield return Get(key, type: type, subgroupIndex: subgroupIndex,
                    shardIndex: shardIndex, version: version);
            }
        }
    }
} // namespace Derecho.Cascade
