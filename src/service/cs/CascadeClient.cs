using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Collections.Generic;

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
            // TODO: needs to be freed after use.
            public IntPtr bytes;
            public UInt64 bytes_size;
            public Int64 version;
            public UInt64 timestamp;
            public Int64 previous_version;
            public Int64 previous_version_by_key;
            public UInt64 message_id;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct VersionTimestampPair
        {
            public Int64 version;
            public UInt64 timestamp;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct PolicyMetadata
        {
            string policyString;
            ShardMemberSelectionPolicy policy;
            UInt32 userNode;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct StdVectorWrapper
        {
            public IntPtr data;
            public IntPtr vecBasePtr;
            public UInt64 length;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct TwoDimensionalNodeList
        {
            public StdVectorWrapper flattenedList;
            public StdVectorWrapper vectorSizes;
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
        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern StdVectorWrapper indexTwoDimensionalNodeVector(IntPtr vec, UInt64 index);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern ObjectProperties extractObjectPropertiesFromQueryResults(IntPtr queryResultsPtr);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern VersionTimestampPair extractVersionTimestampFromQueryResults(IntPtr queryResultsPtr);
        
        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern bool freeVectorPointer(IntPtr ptr);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        public static extern bool freeBytePointer(IntPtr ptr);

        /**
         * Cascade C# Service Client Exported Functions (from unmanaged code)
         */
        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_getServiceClientRef();

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getMyId(IntPtr capi);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getSubgroupIndex(IntPtr capi, string serviceType); 

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_get(IntPtr capi, string key, GetArgs args);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static unsafe extern IntPtr EXPORT_put(IntPtr capi, string object_pool_path, byte[] bytes, UInt64 bytesSize, PutArgs args);      

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr EXPORT_createObjectPool(IntPtr capi, string objectPoolPathname, 
            string serviceType, UInt32 subgroupIndex, string affinitySetRegex);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getNumberOfSubgroups(IntPtr capi, string serviceType);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern UInt32 EXPORT_getNumberOfShards(IntPtr capi, string serviceType, UInt32 shardIndex);
    
        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getMembers(IntPtr capi);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern TwoDimensionalNodeList EXPORT_getSubgroupMembers(IntPtr capi, string serviceType, UInt32 subgroupIndex);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern TwoDimensionalNodeList EXPORT_getSubgroupMembersByObjectPool(IntPtr capi, string objectPoolPathname);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getShardMembers(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern StdVectorWrapper EXPORT_getShardMembersByObjectPool(IntPtr capi, string objectPoolPathname, UInt32 shardIndex);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern void EXPORT_setMemberSelectionPolicy(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex, string policy, UInt32 userNode);

        [DllImport("libcascade_client_cs.so", CallingConvention = CallingConvention.Cdecl)]
        private static extern PolicyMetadata EXPORT_getMemberSelectionPolicy(IntPtr capi, string serviceType, UInt32 subgroupIndex, UInt32 shardIndex);

        /************************
         * Class helpers
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

            freeVectorPointer(vector.vecBasePtr);
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
            
            freeVectorPointer(flattenedList.vecBasePtr);
            freeVectorPointer(vectorSizes.vecBasePtr);
            return outerList;
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
        /// <returns>A VersionTimestampPair response.</returns>
        /// </summary>
        public VersionTimestampPair Put(string key, 
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
            return extractVersionTimestampFromQueryResults(
                EXPORT_put(capi, key, bytes, (UInt64) bytes.Length, args));
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
        /// <param><c>type</c> is the service type. </param>
        /// <param><c>objectPoolPathname</c></param>
        /// <param><c>shardIndex</c></param>
        /// <returns>A list of node IDs of the members.</returns>
        /// </summary>
        public List<UInt32> GetShardMembersByObjectPool(SubgroupType type, 
                                                        string objectPoolPathname,
                                                        UInt32 shardIndex)
        {
            StdVectorWrapper vector = EXPORT_getShardMembersByObjectPool(capi, 
                subgroupEnumToString(type), shardIndex);
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
            return EXPORT_getMemberSelectionPolicy(capi, subgroupEnumToString(type), subgroupIndex, 
                shardIndex);
        } 
    }
} // namespace Derecho.Cascade
