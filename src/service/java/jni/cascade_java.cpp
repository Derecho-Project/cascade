#include <cascade/service_client_api.hpp>
#include "io_cascade_Client.h"
#include "io_cascade_QueryResults.h"

/**
 * macros for handling multiple subgroup types
 */
#define on_subgroup_type(x, ft, ...)             \
    if ((x) == "VCSS")                           \
    {                                            \
        ft<derecho::cascade::VolatileCascadeStoreWithStringKey>(__VA_ARGS__); \
    }                                            \
    else if ((x) == "PCSS")                      \
    {                                            \
        ft<derecho::cascade::PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    }

#define on_service_val(service_val, ft, ...)        \
    switch (service_val)                             \
    {                                                \
    case 0:                                          \
        ft<derecho::cascade::VolatileCascadeStoreWithStringKey>(__VA_ARGS__);     \
        break;                                       \
    case 1:                                          \
        ft<derecho::cascade::PersistentCascadeStoreWithStringKey>(__VA_ARGS__);     \
        break;                                       \
    default:                                         \
        break;                                       \
    }

std::string type_arr[2] = {"VCSS", "PCSS"};

/*
 * Class:     io_cascade_Client
 * Method:    createClient
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_io_cascade_Client_createClient(JNIEnv *env, jobject obj)
{
    // create the service client API
    derecho::cascade::ServiceClientAPI *capi = new derecho::cascade::ServiceClientAPI();
    // send the memory address back as a handle
    return reinterpret_cast<jlong>(capi);
}

/**
 * get the client API from the Java handle.
 */
derecho::cascade::ServiceClientAPI *get_api(JNIEnv *env, jobject obj)
{
    jclass client_cls = env->GetObjectClass(obj);
    jfieldID client_fid = env->GetFieldID(client_cls, "handle", "J");
    jlong jhandle = env->GetLongField(obj, client_fid);
    return reinterpret_cast<derecho::cascade::ServiceClientAPI *>(jhandle);
}

/**
 * Translate a C++ int vector to a Java Integer List.
 */
jobject cpp_int_vector_to_java_list(JNIEnv *env, std::vector<node_id_t> vec)
{
    // create a Java array list
    jclass arr_list_cls = env->FindClass("java/util/ArrayList");
    jmethodID arr_init_mid = env->GetMethodID(arr_list_cls, "<init>", "()V");
    jobject arr_obj = env->NewObject(arr_list_cls, arr_init_mid);

    // list add method
    jclass list_cls = env->FindClass("java/util/List");
    jmethodID list_add_mid = env->GetMethodID(list_cls, "add", "(Ljava/lang/Object;)Z");

    // integer class
    jclass integer_cls = env->FindClass("java/lang/Integer");
    jmethodID integer_init_mid = env->GetMethodID(integer_cls, "<init>", "(I)V");

    // fill everything in
    for (node_id_t id : vec)
    {
        jobject int_obj = env->NewObject(integer_cls, integer_init_mid, id);
        env->CallObjectMethod(arr_obj, list_add_mid, int_obj);
    }
    return arr_obj;
}

/*
 * Class:     io_cascade_Client
 * Method:    getMembers
 * Signature: ()Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_io_cascade_Client_getMembers(JNIEnv *env, jobject obj)
{
    // get members first!
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    std::vector<node_id_t> members = capi->get_members();

    // create an array list
    return cpp_int_vector_to_java_list(env, members);
}

/*
 * Class:     io_cascade_Client
 * Method:    getShardMembers
 * Signature: (JJ)Ljava/util/List;
 */
/**
JNIEXPORT jobject JNICALL Java_io_cascade_Client_getShardMembers__JJ(JNIEnv *env, jobject obj, jlong subgroupID, jlong shardID)
{
    // get shard members in C++
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    std::vector<node_id_t> members = capi->get_shard_members(subgroupID, shardID);

    // create an array list
    return cpp_int_vector_to_java_list(env, members);
}
*/

/**
 * Get the value for any Java object with an integer getValue() function.
 */
int get_value(JNIEnv *env, jobject service_type)
{
    jclass service_types_cls = env->GetObjectClass(service_type);
    jmethodID get_val_mid = env->GetMethodID(service_types_cls, "getValue", "()I");
    return static_cast<int>(env->CallIntMethod(service_type, get_val_mid));
}

/*
 * Class:     io_cascade_Client
 * Method:    getShardMembers
 * Signature: (Lio/cascade/ServiceType;JJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_io_cascade_Client_getShardMembers(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index)
{
    // get the value of the service type
    int val = get_value(env, service_type);
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    std::vector<node_id_t> members;

    // translate the type into string
    std::string real_type = type_arr[val];

    // call get shard members
    on_subgroup_type(real_type, members = capi->get_shard_members, subgroup_index, shard_index);

    // convert the result back to Java list
    return cpp_int_vector_to_java_list(env, members);
}

/*
 * Class:     io_cascade_Client
 * Method:    setMemberSelectionPolicy
 * Signature: (Lio/cascade/ServiceType;JJLio/cascade/ShardMemberSelectionPolicy;)V
 */
JNIEXPORT void JNICALL Java_io_cascade_Client_setMemberSelectionPolicy(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index, jobject java_policy)
{
    int val = get_value(env, service_type);
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    int pol = get_value(env, java_policy);
    derecho::cascade::ShardMemberSelectionPolicy policy = static_cast<derecho::cascade::ShardMemberSelectionPolicy>(pol);

    // translate policy
    int special_node = INVALID_NODE_ID;

    // get the user specified value if the policy is user specified
    if (policy == derecho::cascade::ShardMemberSelectionPolicy::UserSpecified)
    {
        jclass policy_cls = env->GetObjectClass(java_policy);
        jmethodID get_unode_mid = env->GetMethodID(policy_cls, "getUNode", "()I");
        special_node = env->CallIntMethod(java_policy, get_unode_mid);
    }

    std::string real_type = type_arr[val];

    // set!
    on_subgroup_type(real_type, capi->set_member_selection_policy, subgroup_index, shard_index, policy, special_node);
}

/*
 * Class:     io_cascade_Client
 * Method:    getMemberSelectionPolicy
 * Signature: (Lio/cascade/ServiceType;JJ)Lio/cascade/ShardMemberSelectionPolicy;
 */
JNIEXPORT jobject JNICALL Java_io_cascade_Client_getMemberSelectionPolicy(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index)
{
    int val = get_value(env, service_type);
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    std::tuple<derecho::cascade::ShardMemberSelectionPolicy, unsigned int> policy;

    std::string real_type = type_arr[val];

    // get the policy
    on_subgroup_type(real_type, policy = capi->get_member_selection_policy, subgroup_index, shard_index);

    std::string java_policy_str;
    switch (std::get<0>(policy))
    {
    case derecho::cascade::ShardMemberSelectionPolicy::FirstMember:
        java_policy_str = "FirstMember";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::LastMember:
        java_policy_str = "LastMember";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::Random:
        java_policy_str = "Random";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::FixedRandom:
        java_policy_str = "FixedRandom";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::RoundRobin:
        java_policy_str = "RoundRobin";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::UserSpecified:
        java_policy_str = "UserSpecified";
        break;
    case derecho::cascade::ShardMemberSelectionPolicy::InvalidPolicy:
        java_policy_str = "InvalidPolicy";
        break;
    }
    // set the Java policy
    jclass policycls = env->FindClass("io/cascade/ShardMemberSelectionPolicy");

    jfieldID fid = env->GetStaticFieldID(policycls, java_policy_str.c_str(), "Lio/cascade/ShardMemberSelectionPolicy;");

    jobject ret = env->GetStaticObjectField(policycls, fid);

    // set Java side especially when the policy is user specified
    if (std::get<0>(policy) == derecho::cascade::ShardMemberSelectionPolicy::UserSpecified)
    {
        // set the user specified node.
        jmethodID mid = env->GetMethodID(policycls, "setUNode", "(I)V");
        env->CallVoidMethod(ret, mid, static_cast<jint>(std::get<1>(policy)));
    }

    return ret;
}

/** 
 * QueryResultHolder stores the handle for the return value of QueryResults object.
 */
template <typename T>
class QueryResultHolder
{
    // the unique pointer to hold Query Result future of C++.
    derecho::rpc::QueryResults<T> query_result;
    // the dynamic type of the object to be stored in the query results holder
    const std::type_info &storage_type;

public:
    // constructor, _query_result is moved to this->query_result.
    QueryResultHolder(derecho::rpc::QueryResults<T> &_query_result) : query_result(std::move(_query_result)), storage_type(typeid(T)) {}

    // get query_result pointer from java handle
    derecho::rpc::QueryResults<T> *get_result()
    {
        return &query_result;
    }

    const std::type_info &get_type()
    {
        return storage_type;
    }
};

/**
 * Translate Java byte buffer key into std::string keys by their byte values.
 */
std::string translate_str_key(JNIEnv *env, jobject key)
{
    // get long key from byte buffer
// #ifndef NDEBUG
//     std::cout << "translating string key!" << std::endl;
// #endif

    // get the key and store it in a string
    char *key_buf = static_cast<char *>(env->GetDirectBufferAddress(key));
    jlong key_len = env->GetDirectBufferCapacity(key);
    std::string s(key_buf, static_cast<uint64_t>(key_len));
    return s;
}

/**
 * Translate Java key-value pair into C++ object with std::string keys.
 */
derecho::cascade::ObjectWithStringKey *translate_str_obj(JNIEnv *env, jobject key, jobject val)
{
    // get val from byte buffer
    const char *buf = static_cast<const char *>(env->GetDirectBufferAddress(val));
    jlong len = env->GetDirectBufferCapacity(val);
// #ifndef NDEBUG
//     std::cout << "translating string object!" << std::endl;
// #endif

    // translate the object.
    derecho::cascade::ObjectWithStringKey *cas_obj = new derecho::cascade::ObjectWithStringKey();
    cas_obj->key = translate_str_key(env, key);
    cas_obj->blob = derecho::cascade::Blob(buf, len);

    return cas_obj;
}

/**
 * Helper function to put an object into cascade store.
 * @param f a lambda function that converts Java objects into C++ objects.
 * @param env the Java environment to find JVM.
 * @param capi the service client API for this client.
 * @param subgroup_index the subgroup index to put the object.
 * @param shard_index the subgroup index to put the object.
 * @param key the Java byte buffer key to put.
 * @param val the Java byte buffer value to put.
 * @return a handle of the future that stores the version and timestamp.
 */
template <typename T>
jlong put(std::function<typename T::ObjectType *(JNIEnv *, jobject, jobject)> f, JNIEnv *env, derecho::cascade::ServiceClientAPI *capi, jlong subgroup_index, jlong shard_index, jobject key, jobject val)
{
// #ifndef NDEBUG
//     std::cout << "entering jlong put! Here am I! " << std::endl;

//     char *key_buf = static_cast<char *>(env->GetDirectBufferAddress(key));
//     jlong len = env->GetDirectBufferCapacity(key);
//     for (int i = 0; i < len; ++i){
//         printf("%d,", key_buf[i]);
//     }
//     printf("\n");
// #endif
    // translate Java objects to C++ objects.
    typename T::ObjectType *obj = f(env, key, val);
    // std::cout << "putting! " << subgroup_index << " " << shard_index << std::endl;
    // execute the put
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> res = capi->put<T>(*obj, subgroup_index, shard_index);
    //store the result in a handler and return it!
    // std::cout << "finished put!" << std::endl;
    QueryResultHolder<std::tuple<persistent::version_t, uint64_t>> *qrh = new QueryResultHolder<std::tuple<persistent::version_t, uint64_t>>(res);
    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_cascade_Client
 * Method:    putInternal
 * Signature: (Lio/cascade/ServiceType;JJLjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_io_cascade_Client_putInternal(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index, jobject key, jobject val)
{
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    int service_val = get_value(env, service_type);

#ifndef NDEBUG
    // std::cout << "service value:" << service_val << std::endl;
#endif

    // executing the put
    on_service_val(service_val, return put, translate_str_obj, env, capi, subgroup_index, shard_index, key, val);

    // if service_val does not match successfully, return -1
    return -1;
}

/**
 * Helper function to get an object from a cascade store.
 * @param f a lambda function that converts Java keys into C++ keys.
 * @param env the Java environment to find JVM.
 * @param capi the service client API for this client.
 * @param subgroup_index the subgroup index to get the object.
 * @param shard_index the subgroup index to get the object.
 * @param key the Java byte buffer key to get.
 * @param ver the version number of the key-value pair to get.
 * @return a handle of the future that stores the buffer of the value.
 */
template <typename T>
jlong get(JNIEnv *env, derecho::cascade::ServiceClientAPI *capi, jlong subgroup_index, jlong shard_index, jobject key, jlong ver, std::function<typename T::KeyType(JNIEnv *, jobject)> f)
{
// #ifndef NDEBUG
//     std::cout << "start get!" << std::endl;
// #endif
    // translate the key
    typename T::KeyType obj = f(env, key);
    // execute get
    derecho::rpc::QueryResults<const typename T::ObjectType> res = capi->get<T>(obj, ver, subgroup_index, shard_index);
    // store the result in a handler
    QueryResultHolder<const typename T::ObjectType> *qrh = new QueryResultHolder<const typename T::ObjectType>(res);
    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_cascade_Client
 * Method:    getInternal
 * Signature: (Lio/cascade/ServiceType;JJLjava/nio/ByteBuffer;J)J
 */
JNIEXPORT jlong JNICALL Java_io_cascade_Client_getInternal(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index, jobject key, jlong version)
{
// #ifndef NDEBUG
//     std::cout << "start get internal!" << std::endl;
// #endif
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    int service_val = get_value(env, service_type);

    // executing the get
    on_service_val(service_val, return get, env, capi, subgroup_index, shard_index, key, version, translate_str_key);

    // if service_val does not match successfully, return -1
    return -1;
}

/**
 * Helper function to get an object from a cascade store by time.
 * @param f a lambda function that converts Java keys into C++ keys.
 * @param env the Java environment to find JVM.
 * @param capi the service client API for this client.
 * @param subgroup_index the subgroup index to get the object.
 * @param shard_index the subgroup index to get the object.
 * @param key the Java byte buffer key to get.
 * @param timestamp the timestamp of the key-value pair to get.
 * @return a handle of the future that stores the buffer of the value.
 */
template <typename T>
jlong get_by_time(JNIEnv *env, derecho::cascade::ServiceClientAPI *capi, jlong subgroup_index, jlong shard_index, jobject key, jlong timestamp, std::function<typename T::KeyType(JNIEnv *, jobject)> f)
{
    // translate the key
    typename T::KeyType obj = f(env, key);
    // execute get
    derecho::rpc::QueryResults<const typename T::ObjectType> res = capi->get_by_time<T>(obj, timestamp, subgroup_index, shard_index);
    // store the result in a handler
    QueryResultHolder<const typename T::ObjectType> *qrh = new QueryResultHolder<const typename T::ObjectType>(res);
    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_cascade_Client
 * Method:    getInternalByTime
 * Signature: (Lio/cascade/ServiceType;JJLjava/nio/ByteBuffer;J)J
 */
JNIEXPORT jlong JNICALL Java_io_cascade_Client_getInternalByTime(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index, jobject key, jlong timestamp)
{
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    int service_val = get_value(env, service_type);

    on_service_val(service_val, return get_by_time, env, capi, subgroup_index, shard_index, key, timestamp, translate_str_key);

    return -1;
}

/**
 * Helper function to remove an object from a cascade store by time.
 * @param f a lambda function that converts Java keys into C++ keys.
 * @param env the Java environment to find JVM.
 * @param capi the service client API for this client.
 * @param subgroup_index the subgroup index to get the object.
 * @param shard_index the subgroup index to get the object.
 * @param key the Java byte buffer key to get.
 * @return a handle of the future that stores the version and timestamp of the operation.
 */
template <typename T>
jlong remove(JNIEnv *env, derecho::cascade::ServiceClientAPI *capi, jlong subgroup_index, jlong shard_index, jobject key, std::function<typename T::KeyType(JNIEnv *, jobject)> f)
{
    // translate the key
    typename T::KeyType obj = f(env, key);
    // execute remove
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> res = capi->remove<T>(obj, subgroup_index, shard_index);
    // store the results in a handle
    QueryResultHolder<std::tuple<persistent::version_t, uint64_t>> *qrh = new QueryResultHolder<std::tuple<persistent::version_t, uint64_t>>(res);
    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_cascade_Client
 * Method:    removeInternal
 * Signature: (Lio/cascade/ServiceType;JJLjava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_io_cascade_Client_removeInternal(JNIEnv *env, jobject obj, jobject service_type, jlong subgroup_index, jlong shard_index, jobject key)
{
    derecho::cascade::ServiceClientAPI *capi = get_api(env, obj);
    int service_val = get_value(env, service_type);

    on_service_val(service_val, return remove, env, capi, subgroup_index, shard_index, key, translate_str_key);

    return -1;
}

/**
 * Put objects acquired from the future into a Java hash map.
 * @param env the Java environment
 * @param handle the handle for the query result future
 * @param hashmap the Java hashmap to put results
 * @param f a lambda that translates particular types of query results into Java objects.
 */
template <typename T>
void create_object_from_query(JNIEnv *env, jlong handle, jobject hashmap, std::function<jobject(T)> f)
{
    // get the put function of a hash map
    jclass hash_map_cls = env->GetObjectClass(hashmap);
    jmethodID map_put = env->GetMethodID(hash_map_cls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    // fetch the results from the future
    typedef QueryResultHolder<T> query;
    query *qrh = reinterpret_cast<query *>(handle);
    derecho::rpc::QueryResults<T> *result = qrh->get_result();
    for (auto &reply_pair : result->get())
    {
        // translate the key to Java through Integer class
        jclass integer_class = env->FindClass("java/lang/Integer");
        jmethodID integer_constructor = env->GetMethodID(integer_class, "<init>", "(I)V"); // changed init integer_class
        jobject integer_object = env->NewObject(integer_class, integer_constructor, static_cast<jint>(reply_pair.first));

#ifndef NDEBUG
        // std::cout << "trying to get the promise..." << std::endl;
#endif

        // translate the value using lambda
        T obj = reply_pair.second.get();
        jobject java_obj = f(obj);

        // put the key value pair into the hashmap
        env->CallObjectMethod(hashmap, map_put, integer_object, java_obj);
    }
}

/*
 * Class:     io_cascade_QueryResults
 * Method:    getReplyMap
 * Signature: (J)Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_io_cascade_QueryResults_getReplyMap(JNIEnv *env, jobject obj, jlong handle)
{

    // Process the query results into a java map
    jclass hash_map_cls = env->FindClass("java/util/HashMap");

    jmethodID hash_map_constructor = env->GetMethodID(hash_map_cls, "<init>", "()V");
    jobject hash_map_object = env->NewObject(hash_map_cls, hash_map_constructor);

    // get the mode and the type of the object
    jclass querycls = env->GetObjectClass(obj);
    jfieldID mode_fid = env->GetFieldID(querycls, "mode", "I");
    jint mode = env->GetIntField(obj, mode_fid);
    jfieldID type_fid = env->GetFieldID(querycls, "type", "Lio/cascade/ServiceType;");
    jobject type = env->GetObjectField(obj, type_fid);

    int type_val = get_value(env, type);

// #ifndef NDEBUG
//     std::cout << "in reply map: value is " << type_val << " and mode is " << (int)mode << std::endl;
// #endif
    // lambda that translates into bundle types
    auto bundle_f = [env](std::tuple<persistent::version_t, uint64_t> obj) {

// #ifndef NDEBUG   
//         std::cout << "converting bundles with string keys!" << std::endl;
// #endif

        // get the version and timestamp pair
        persistent::version_t ver = std::get<0>(obj);
        uint64_t timestamp = std::get<1>(obj);

// #ifndef NDEBUG
//         std::cout << "version: " << ver << "; timestamp: " << timestamp << std::endl;
// #endif

        // creates the bundle object
        jclass bundle_class = env->FindClass("io/cascade/Bundle");
        jmethodID bundle_constructor = env->GetMethodID(bundle_class, "<init>", "(JJ)V"); // changed init integer_class
        return env->NewObject(bundle_class, bundle_constructor, static_cast<jlong>(ver), static_cast<jlong>(timestamp));
    };

    // lambda that translates into byte buffer types and receives objects with uint64 keys.
/*    auto u64_f = [env](derecho::cascade::ObjectWithUInt64Key obj) {
        char *data = obj.blob.bytes;
        std::size_t size = obj.blob.size;

        // initialize the Java byte array
        jbyteArray data_byte_arr = env->NewByteArray(size);
        env->SetByteArrayRegion(data_byte_arr, 0, size, reinterpret_cast<jbyte *>(data));

        jclass byte_buffer_cls = env->FindClass("java/nio/ByteBuffer");
        // create and return a new direct byte buffer
        jmethodID alloc_mid = env->GetStaticMethodID(byte_buffer_cls, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");
        jobject byte_buf_obj = env->CallStaticObjectMethod(byte_buffer_cls, alloc_mid, static_cast<jint>(size));
        jmethodID put_mid = env->GetMethodID(byte_buffer_cls, "put", "([B)Ljava/nio/ByteBuffer;");
        env->CallObjectMethod(byte_buf_obj, put_mid, data_byte_arr);
        return byte_buf_obj;
    };
*/
    auto s_f = [env](derecho::cascade::ObjectWithStringKey obj) {
// #ifndef NDEBUG
//         std::cout << "converting objects with string keys!" << std::endl;
// #endif

        char *data = obj.blob.bytes;
        std::size_t size = obj.blob.size;

        // initialize the java byte array
        jbyteArray data_byte_arr = env->NewByteArray(size);

        env->SetByteArrayRegion(data_byte_arr, 0, size, reinterpret_cast<jbyte *>(data));

        jclass byte_buffer_cls = env->FindClass("java/nio/ByteBuffer");
        // create and return a new direct byte buffer
        jmethodID alloc_mid = env->GetStaticMethodID(byte_buffer_cls, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");
        jobject byte_buf_obj = env->CallStaticObjectMethod(byte_buffer_cls, alloc_mid, static_cast<jint>(size));
        jmethodID put_mid = env->GetMethodID(byte_buffer_cls, "put", "([B)Ljava/nio/ByteBuffer;");
        env->CallObjectMethod(byte_buf_obj, put_mid, data_byte_arr);
        return byte_buf_obj;
    };

    // get different reply maps base on mode
    switch (mode)
    {
    case 0:
        create_object_from_query<std::tuple<persistent::version_t, uint64_t>>(env, handle, hash_map_object, bundle_f);
        break;
    case 1:
        switch (type_val)
        {
        case 0:
        case 1:
/*            create_object_from_query<const derecho::cascade::ObjectWithUInt64Key>(env, handle, hash_map_object, u64_f);
            break;
        case 2:
        case 3:
*/
            create_object_from_query<const derecho::cascade::ObjectWithStringKey>(env, handle, hash_map_object, s_f);
            break;
        }
        break;
    default:
        break;
    }
    return hash_map_object;
}
