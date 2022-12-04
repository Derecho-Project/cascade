#pragma once
#include "cascade/object_pool_metadata.hpp"
#include <atomic>
#include <cascade/service_client_api.hpp>
#include <derecho/utils/logger.hpp>
#include <fuse3/fuse_lowlevel.h>
#include <limits>
#include <memory>
#include <mutex>
#include <mutils-containers/KindMap.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <time.h>

#include <unistd.h>
#define GetCurrentDir getcwd

namespace derecho {
namespace cascade {

using json = nlohmann::json;
#define FUSE_CLIENT_DEV_ID (0xCA7CADE)
#define FUSE_CLIENT_BLK_SIZE (4096)

#define META_FILE_NAME ".cascade"

#define TO_FOREVER (std::numeric_limits<double>::max())

typedef enum {
    SITE = 0,
    CASCADE_TYPE,
    SUBGROUP,
    SHARD,
    OBJECTPOOL_PATH,
    KEY,
    META,
    METADATA_SERVICE,
    DATAPATH_LOGIC,
    DLL,
} INodeType;

class FileBytes {
public:
    size_t size;
    uint8_t* bytes;
    FileBytes() : size(0), bytes(nullptr) {}
    FileBytes(size_t s) : size(s) {
        bytes = nullptr;
        if(s > 0) {
            bytes = (uint8_t*)malloc(s);
        }
    }
    virtual ~FileBytes() {
        if(bytes) {
            free(bytes);
        }
    }
};

class FuseClientINode {
public:
    INodeType type;
    std::string display_name;
    std::vector<std::unique_ptr<FuseClientINode>> children;
    std::shared_mutex children_mutex;
    fuse_ino_t parent;
    time_t update_interval;
    time_t last_update_sec;

    /**
     * get directory entries. This is the default implementation.
     * Override it as required.
     */
    virtual std::map<std::string, fuse_ino_t> get_dir_entries() {
        std::map<std::string, fuse_ino_t> ret_map;
        for(auto& child : this->children) {
            ret_map.emplace(child->display_name, reinterpret_cast<fuse_ino_t>(child.get()));
        }
        return ret_map;
    }

    virtual uint64_t get_file_size() {
        return sizeof(*this);
    }

    virtual uint64_t read_file(FileBytes* fb) {
        (void)fb;
        return 0;
    }

    virtual void initialize() {
    }

    // Helper function for get_dir_entries() and read_file()
    void check_update() {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        if(now.tv_sec > (last_update_sec + update_interval)) {
            clock_gettime(CLOCK_REALTIME, &now);
            if(now.tv_sec > (last_update_sec + update_interval)) {
                last_update_sec = now.tv_sec;
                update_contents();
            }
        }
    }

private:
    // Helper functions for check_update()
    virtual void update_contents() {
        return;
    }
};

template <typename T>
struct TypeName { static const char* name; };

template <typename T>
const char* TypeName<T>::name = "unknown";

template <>
const char* TypeName<VolatileCascadeStoreWithStringKey>::name = "VolatileCascadeStoreWithStringKey";

template <>
const char* TypeName<PersistentCascadeStoreWithStringKey>::name = "PersistentCascadeStoreWithStringKey";

template <>
const char* TypeName<TriggerCascadeNoStoreWithStringKey>::name = "TriggerCascadeNoStoreWithStringKey";

#define CONF_LAYOUT "layout"

template <typename CascadeType>
class SubgroupINode;

template <typename CascadeType>
class ShardINode;

template <typename CascadeType>
class KeyINode;

class RootMetaINode;

template <typename CascadeType>
class ShardMetaINode;

class ObjectPoolRootINode;

class ObjectPoolPathINode;

class ObjectPoolKeyINode;

class ObjectPoolMetaINode;

class MetadataServiceRootINode;

class DataPathLogicRootINode;

template <typename CascadeType>
class DLLINode;

template <typename CascadeType>
class CascadeTypeINode : public FuseClientINode {
public:
    CascadeTypeINode() {
        this->type = INodeType::CASCADE_TYPE;
        this->display_name = std::string(TypeName<CascadeType>::name);
        this->parent = FUSE_ROOT_ID;
    }

    /** initialize */
    void initialize(const json& group_layout, ServiceClientAPI& capi) {
        this->display_name = group_layout["type_alias"];
        uint32_t sidx = 0;
        for(auto subgroup_it : group_layout[CONF_LAYOUT]) {
            children.emplace_back(std::make_unique<SubgroupINode<CascadeType>>(sidx, reinterpret_cast<fuse_ino_t>(this)));
            size_t num_shards = subgroup_it[MIN_NODES_BY_SHARD].size();
            for(uint32_t shidx = 0; shidx < num_shards; shidx++) {
                this->children[sidx]->children.emplace_back(
                        std::make_unique<ShardINode<CascadeType>>(
                                shidx, reinterpret_cast<fuse_ino_t>(this->children[sidx].get()), capi));
            }
            sidx++;
        }
    }
};

class RootMetaINode : public FuseClientINode {
    ServiceClientAPI& capi;
    std::string contents;

public:
    RootMetaINode(ServiceClientAPI& _capi) : capi(_capi) {
        this->update_interval = 2;
        this->last_update_sec = 0;
        this->type = INodeType::META;
        this->display_name = META_FILE_NAME;
    }

    virtual uint64_t get_file_size() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        check_update();
        return contents.size();
    }

private:
    virtual void update_contents() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        auto members = capi.get_members();
        contents = "number of nodes in cascade service: " + std::to_string(members.size()) + ".\nnode IDs: ";
        for(auto& nid : members) {
            contents += std::to_string(nid) + ",";
        }
        contents += "\n";

        auto objectpools = capi.list_object_pools(true);
        contents += "number of objectpool in cascade service: " + std::to_string(objectpools.size()) + ".\nObjectpool paths: ";
        for(auto& objectpool_path : objectpools) {
            contents += objectpool_path + ",";
        }
        contents += "\n";
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        this->check_update();
        file_bytes->size = contents.size();
        file_bytes->bytes = reinterpret_cast<uint8_t*>(strdup(contents.c_str()));
        return 0;
    }
};

template <typename CascadeType>
class SubgroupINode : public FuseClientINode {
public:
    const uint32_t subgroup_index;

    SubgroupINode(uint32_t sidx, fuse_ino_t pino) : subgroup_index(sidx) {
        this->type = INodeType::SUBGROUP;
        this->display_name = "subgroup-" + std::to_string(sidx);
        this->parent = pino;
    }
};

template <typename CascadeType>
class ShardINode : public FuseClientINode {
public:
    const uint32_t shard_index;
    ServiceClientAPI& capi;
    std::map<typename CascadeType::KeyType, fuse_ino_t> key_to_ino;

    ShardINode(uint32_t shidx, fuse_ino_t pino, ServiceClientAPI& _capi) : shard_index(shidx), capi(_capi) {
        this->type = INodeType::SHARD;
        this->display_name = "shard-" + std::to_string(shidx);
        this->parent = pino;
        SubgroupINode<CascadeType>* p_subgroup_inode = reinterpret_cast<SubgroupINode<CascadeType>*>(pino);
        this->children.emplace_back(std::make_unique<ShardMetaINode<CascadeType>>(shidx, p_subgroup_inode->subgroup_index, capi));
    }

    // TODO: rethinking about the consistency
    virtual std::map<std::string, fuse_ino_t> get_dir_entries() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        // std::map<std::string,fuse_ino_t> ret_map;
        /** we always retrieve the key list for a shard inode because the data is highly dynamic */
        uint32_t subgroup_index = reinterpret_cast<SubgroupINode<CascadeType>*>(this->parent)->subgroup_index;
        auto result = capi.template list_keys<CascadeType>(CURRENT_VERSION, true, subgroup_index, this->shard_index);
        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            std::unique_lock wlck(this->children_mutex);
            for(auto& key : reply) {
                if(key_to_ino.find(key) == key_to_ino.end()) {
                    this->children.emplace_back(std::make_unique<KeyINode<CascadeType>>(key, reinterpret_cast<fuse_ino_t>(this), capi));
                    // To solve the issue of '/' in display name , which will cause: reading directory '.': input/output error
                    // TODO: if there are better replacement of /, instead of -
                    key = std::string("key-") + key;
                    std::replace(key.begin(), key.end(), '/', '-');
                    key_to_ino[key] = reinterpret_cast<fuse_ino_t>(this->children.back().get());
                }
            }
        }
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return FuseClientINode::get_dir_entries();
    }
};

template <>
class ShardINode<TriggerCascadeNoStoreWithStringKey> : public FuseClientINode {
public:
    const uint32_t shard_index;
    ServiceClientAPI& capi;
    std::map<TriggerCascadeNoStoreWithStringKey::KeyType, fuse_ino_t> key_to_ino;

    ShardINode(uint32_t shidx, fuse_ino_t pino, ServiceClientAPI& _capi) : shard_index(shidx), capi(_capi) {
        this->type = INodeType::SHARD;
        this->display_name = "shard-" + std::to_string(shidx);
        this->parent = pino;
        SubgroupINode<TriggerCascadeNoStoreWithStringKey>* p_subgroup_inode = reinterpret_cast<SubgroupINode<TriggerCascadeNoStoreWithStringKey>*>(pino);
        this->children.emplace_back(std::make_unique<ShardMetaINode<TriggerCascadeNoStoreWithStringKey>>(shidx, p_subgroup_inode->subgroup_index, capi));
    }

    virtual std::map<std::string, fuse_ino_t> get_dir_entries() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        std::map<std::string, fuse_ino_t> ret_map;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return ret_map;
    }
};

template <typename CascadeType>
class ShardMetaINode : public FuseClientINode {
private:
    const uint32_t shard_index;
    const uint32_t subgroup_index;
    ServiceClientAPI& capi;
    std::string contents;

    /**
     * update the metadata. need write lock.
     */
    virtual void update_contents() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        auto members = capi.template get_shard_members<CascadeType>(subgroup_index, shard_index);
        contents = "number of nodes shard: " + std::to_string(members.size()) + ".\nnode IDs: ";
        for(auto& nid : members) {
            contents += std::to_string(nid) + ",";
        }
        contents += "\n";
        auto policy = capi.template get_member_selection_policy<CascadeType>(subgroup_index, shard_index);
        contents += "member selection policy:";
        switch(std::get<0>(policy)) {
            case FirstMember:
                contents += "FirstMember\n";
                break;
            case LastMember:
                contents += "LastMember\n";
                break;
            case Random:
                contents += "Random\n";
                break;
            case FixedRandom:
                contents += "FixedRandom(";
                contents += std::to_string(std::get<1>(policy));
                contents += ")\n";
                break;
            case RoundRobin:
                contents += "RoundRobin\n";
                break;
            case UserSpecified:
                contents += "UserSpecified(";
                contents += std::to_string(std::get<1>(policy));
                contents += ")\n";
                break;
            default:
                contents += "Unknown\n";
                break;
        }
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

public:
    ShardMetaINode(const uint32_t _shard_index, const uint32_t _subgroup_index, ServiceClientAPI& _capi) : shard_index(_shard_index),
                                                                                                           subgroup_index(_subgroup_index),
                                                                                                           capi(_capi) {
        this->update_interval = 2;
        this->last_update_sec = 0;
        this->type = INodeType::META;
        this->display_name = META_FILE_NAME;
    }

    virtual uint64_t get_file_size() override {
        check_update();
        return contents.size();
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        check_update();
        file_bytes->size = contents.size();
        file_bytes->bytes = reinterpret_cast<uint8_t*>(strdup(contents.c_str()));
        return 0;
    }
};

template <typename CascadeType>
class KeyINode : public FuseClientINode {
public:
    typename CascadeType::KeyType key;
    std::unique_ptr<FileBytes> file_bytes;
    uint64_t file_size;
    persistent::version_t version;
    uint64_t timestamp_us;
    persistent::version_t previous_version;
    persistent::version_t previous_version_by_key;  // previous version by key, INVALID_VERSION for the first value of the key.
    ServiceClientAPI& capi;

    KeyINode(typename CascadeType::KeyType& k, fuse_ino_t pino, ServiceClientAPI& _capi) : key(k),
                                                                                           file_bytes(std::make_unique<FileBytes>()),
                                                                                           capi(_capi) {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        this->update_interval = 2;
        this->last_update_sec = 0;
        this->type = INodeType::KEY;
        if constexpr(std::is_same<std::remove_cv_t<typename CascadeType::KeyType>, char*>::value || std::is_same<std::remove_cv_t<typename CascadeType::KeyType>, std::string>::value) {
            this->display_name = std::string("key-") + k;
        } else if constexpr(std::is_arithmetic<std::remove_cv_t<typename CascadeType::KeyType>>::value) {
            this->display_name = std::string("key-") + std::to_string(k);
        } else {
            // KeyType is required to implement to_string() for types other than type string/arithmetic.
            this->display_name = std::string("key-") + key.to_string();
        }
        // '/' in display name, will cause: reading directory '.': input/output error
        std::replace(this->display_name.begin(), this->display_name.end(), '/', '\\');
        this->parent = pino;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

    virtual uint64_t read_file(FileBytes* _file_bytes) override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        check_update();
        _file_bytes->size = this->file_bytes.get()->size;
        _file_bytes->bytes = static_cast<uint8_t*>(malloc(this->file_bytes.get()->size));
        memcpy(_file_bytes->bytes, this->file_bytes.get()->bytes, this->file_bytes.get()->size);
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return 0;
    }

    virtual uint64_t get_file_size() override {
        check_update();
        return this->file_bytes.get()->size;
    }

    KeyINode(KeyINode&& fci) {
        this->type = std::move(fci.type);
        this->display_name = std::move(fci.display_name);
        this->parent = std::move(fci.parent);
        this->key = std::move(fci.key);
        this->capi = std::move(fci.capi);
    }

    virtual ~KeyINode() {
        dbg_default_info("[{}] entering {}.", gettid(), __func__);
        file_bytes.reset(nullptr);
        dbg_default_info("[{}] leaving {}.", gettid(), __func__);
    }

private:
    virtual void update_contents() override {
        ShardINode<CascadeType>* pino_shard = reinterpret_cast<ShardINode<CascadeType>*>(this->parent);
        SubgroupINode<CascadeType>* pino_subgroup = reinterpret_cast<SubgroupINode<CascadeType>*>(pino_shard->parent);
        auto result = capi.template get<CascadeType>(
                key, CURRENT_VERSION, true, pino_subgroup->subgroup_index, pino_shard->shard_index);
        Blob blob;
        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            dbg_default_trace("------ KEY INODE reply {}.", reply);
            if(std::is_base_of<typename CascadeType::ObjectType, ObjectWithStringKey>::value) {
                ObjectWithStringKey* access_ptr = reinterpret_cast<ObjectWithStringKey*>(&reply);
                this->version = access_ptr->version;
                this->timestamp_us = access_ptr->timestamp_us;
                this->previous_version = access_ptr->previous_version;
                this->previous_version_by_key = access_ptr->previous_version_by_key;
                blob = access_ptr->blob;
            } else if(std::is_base_of<typename CascadeType::ObjectType, ObjectWithUInt64Key>::value) {
                ObjectWithUInt64Key* access_ptr = reinterpret_cast<ObjectWithUInt64Key*>(&reply);
                this->version = access_ptr->version;
                this->timestamp_us = access_ptr->timestamp_us;
                this->previous_version = access_ptr->previous_version;
                this->previous_version_by_key = access_ptr->previous_version_by_key;
                blob = access_ptr->blob;
            } else {
                this->file_bytes.get()->size = mutils::bytes_size(reply);
                this->file_bytes.get()->bytes = static_cast<uint8_t*>(malloc(this->file_bytes.get()->size));
                reply.to_bytes(this->file_bytes.get()->bytes);
                return;
            }
            file_bytes.get()->size = blob.size;
            file_bytes.get()->bytes = static_cast<uint8_t*>(malloc(file_bytes.get()->size));
            memcpy(file_bytes.get()->bytes, blob.bytes, file_bytes.get()->size);
            return;
        }
    }
};

class ObjectPoolMetaINode : public FuseClientINode {
private:
    std::string cur_pathname;
    /** objp_collection contains the all the objp with the same cur_pathname prefix
     *  i.e. if cur_path name is "/a",
     *  object pools "/a/b1", "/a/b2" share this level of ObjectPoolPathINode*/
    std::vector<std::string> objp_collection;
    bool is_object_pool;
    uint32_t subgroup_type_index;
    uint32_t subgroup_index;
    sharding_policy_t sharding_policy;
    bool deleted;
    ServiceClientAPI& capi;
    std::string contents;

    virtual void update_contents() override {
        this->contents = "Current Directory Pathname: ";
        this->contents += (cur_pathname == "") ? "objectPoolRoot" : cur_pathname;
        this->contents += "\n";
        this->objp_collection.clear();
        this->contents += "contains the below object pools in its subdirs:\n";
        std::string objp_contents;
        // Check the objectPools under cur_pathname directory
        size_t cur_len = cur_pathname.length();
        for(std::string pathname : capi.list_object_pools(true)) {
            if(cur_pathname.length() == 0 || (pathname.length() > cur_len - 1 && (pathname.substr(0, cur_len) == cur_pathname))) {
                if(pathname.length() == cur_len) {
                    this->objp_collection.emplace_back(pathname);
                    this->contents += " " + pathname + ",\n";
                    this->is_object_pool = true;
                    this->objectPool_contents(objp_contents);
                } else if(pathname[cur_len] == '/') {
                    this->objp_collection.emplace_back(pathname);
                    this->contents += " " + pathname + ",\n";
                }
            }
        }
        this->contents += objp_contents;
    }

    /**
     * Only fill the object pool contents when cur_pathname is an object pool
     */
    void objectPool_contents(std::string& objp_contents) {
        auto op_metadata = capi.find_object_pool(this->cur_pathname);
        objp_contents += "Current Object Pool Pathname: ";
        objp_contents += cur_pathname + "\n";
        this->deleted = op_metadata.deleted;
        objp_contents += "- is deleted: ";
        objp_contents += this->deleted ? "true" : "false";
        objp_contents += "\n";
        this->subgroup_type_index = op_metadata.subgroup_type_index;
        objp_contents += "- subgroup type index: ";
        objp_contents += std::to_string(this->subgroup_type_index) + "\n";
        this->subgroup_index = op_metadata.subgroup_index;
        objp_contents += "- subgroup index: ";
        objp_contents += std::to_string(this->subgroup_index) + "\n";
        auto policy = op_metadata.sharding_policy;
        objp_contents += "- sharding policy: ";
        switch(policy) {
            case HASH:
                objp_contents += "Hashing\n";
                break;
            case RANGE:
                objp_contents += "Range\n";
                break;
            default:
                objp_contents += "Unknown\n";
                break;
        }
    }

public:
    ObjectPoolMetaINode(const std::string _cur_pathname, ServiceClientAPI& _capi) : cur_pathname(_cur_pathname),
                                                                                    capi(_capi) {
        this->update_interval = 2;
        this->last_update_sec = 0;
        this->type = INodeType::META;
        this->display_name = META_FILE_NAME;
    }

    void add_objp(std::string new_objp_pathname) {
        for(auto& pathname : objp_collection) {
            if(pathname == new_objp_pathname) {
                return;
            }
        }
        objp_collection.emplace_back(new_objp_pathname);
    }

    virtual uint64_t get_file_size() override {
        check_update();
        return contents.size();
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        check_update();
        file_bytes->size = contents.size();
        file_bytes->bytes = reinterpret_cast<uint8_t*>(strdup(contents.c_str()));
        return 0;
    }
};

class ObjectPoolPathINode : public FuseClientINode {
public:
    ServiceClientAPI& capi;
    std::string cur_pathname;
    bool is_object_pool;
    std::set<std::string> key_children;
    std::set<std::string> objp_children;

    ObjectPoolPathINode(fuse_ino_t pino, ServiceClientAPI& _capi) : capi(_capi) {
        this->update_interval = 10;
        this->last_update_sec = 0;
        this->type = INodeType::OBJECTPOOL_PATH;
        this->parent = pino;
        this->is_object_pool = false;
        this->cur_pathname = "";
        this->children.emplace_back(std::make_unique<ObjectPoolMetaINode>(cur_pathname, capi));
    }

    ObjectPoolPathINode(std::string _cur_pathname, fuse_ino_t pino, ServiceClientAPI& _capi) : capi(_capi),
                                                                                               cur_pathname(_cur_pathname) {
        this->update_interval = 10;
        this->last_update_sec = 10;
        this->type = INodeType::OBJECTPOOL_PATH;
        std::size_t pos = cur_pathname.find_last_of('/');
        this->display_name = cur_pathname.substr(pos + 1);
        this->parent = pino;
        this->is_object_pool = false;
        // this->objp_collection.emplace_back(objp_pathname);
        this->children.emplace_back(std::make_unique<ObjectPoolMetaINode>(cur_pathname, capi));
    }

    /** Helper function:
     *   @object_pool_pathname: the object pool pathname to parse
     *   @return:  next level pathname
     *   e.g. if cur_pathname is "/a", for object_pool_pathname "/a/b/c" this function returns "/a/b"
     */
    std::string get_next_level_pathname(std::string& object_pool_pathname) {
        size_t cur_len = cur_pathname.length();
        std::string remain_pathname = object_pool_pathname.substr(cur_len);
        auto start_pos = remain_pathname.find("/");
        if(start_pos == std::string::npos) {
            return "";
        }
        // get the next level of directory pathname.
        auto end_pos = remain_pathname.substr(start_pos + 1).find("/");
        std::string next_level_pathname;
        if(end_pos == std::string::npos) {
            next_level_pathname = cur_pathname + remain_pathname.substr(start_pos);
        } else {
            next_level_pathname = cur_pathname + remain_pathname.substr(start_pos, end_pos + 1);
        }
        return next_level_pathname;
    }

    /** Construct the next level objectPoolINodes, starting from remain pathname
     *e.g./a/b/c should have three layers: /a, /a/b, /a/b/c
     *if this->cur_pathname is "/a", whose child INode's cur_pathname is "/a/b", whose child INode "/a/b/c"
     */
    void construct_nextlevel_objectpool_path(std::string& object_pool_pathname) {
        // check path_name
        std::string next_level_pathname = get_next_level_pathname(object_pool_pathname);
        if(next_level_pathname.length() == 0) {
            return;
        }
        // Check if this objectPoolPathInode with the same pathname exists
        // case 1: If this level pathname directory exists
        for(auto& inode : this->children) {
            if(inode->type == INodeType::OBJECTPOOL_PATH) {
                if(static_cast<ObjectPoolPathINode*>(inode.get())->cur_pathname == next_level_pathname) {
                    return;
                }
            }
        }
        // case2: If this level ObjectPoolPathInode doesn't exists, create one
        // std::unique_lock wlck(this->children_mutex);
        this->children.emplace_back(std::make_unique<ObjectPoolPathINode>(next_level_pathname, reinterpret_cast<fuse_ino_t>(this), capi));
        objp_children.insert(cur_pathname);
    }

    void update_objpINodes() {
        size_t cur_len = cur_pathname.length();
        std::vector<std::string> reply = capi.list_object_pools(true);
        std::vector<std::string> valid_subdirs;
        // Check if need to add new ObjectPoolPathINode to children inodes
        for(std::string& object_pool : reply) {
            if(object_pool.length() < cur_len) {
                continue;
            }
            if(object_pool == cur_pathname) {
                this->is_object_pool = true;
                continue;
            }
            bool is_subdir = cur_pathname.length() == 0 || (object_pool.substr(0, cur_len) == cur_pathname && object_pool[cur_len] == '/');
            if(!is_subdir) {
                continue;
            }
            std::string next_level_pathname(get_next_level_pathname(object_pool));
            valid_subdirs.emplace_back(next_level_pathname);
            if(objp_children.find(next_level_pathname) == objp_children.end()) {
                construct_nextlevel_objectpool_path(object_pool);
            }
        }
        // Check if need to remove existing ObjectPoolPathINode from children inodes
        if(std::find(reply.begin(), reply.end(), this->cur_pathname) == reply.end()) {
            this->is_object_pool = false;
        }
        auto it = this->children.begin();
        while(it != this->children.end()) {
            std::string name = cur_pathname + "/" + (*it)->display_name;
            if((*it)->type == INodeType::OBJECTPOOL_PATH && (std::find(valid_subdirs.begin(), valid_subdirs.end(), name) == valid_subdirs.end())) {
                objp_children.erase(objp_children.find(name));
                it = this->children.erase(it);
            } else {
                ++it;
            }
        }
    }

    void update_keyINodes() {
        // case1. if object pool of cur_pathname donesn't exists anymore, remove all the keyINode from children
        if(!this->is_object_pool) {
            auto it = this->children.begin();
            while(it != this->children.end()) {
                if((*it)->type == INodeType::KEY) {
                    it = this->children.erase(it);
                } else {
                    ++it;
                }
            }
            return;
        }
        // case2. if cur_pathname is a valid object pool, refetch keys in this object pool
        persistent::version_t version = CURRENT_VERSION;
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<std::string>>>> future_result = capi.list_keys(version, true, cur_pathname);
        std::vector<std::string> reply = capi.wait_list_keys<std::string>(future_result);
        // Check if need to add new keyINode to children inodes
        for(auto& key : reply) {
            if(key_children.find(key) == key_children.end()) {
                this->children.emplace_back(std::make_unique<ObjectPoolKeyINode>(key, reinterpret_cast<fuse_ino_t>(this), capi));
                key_children.insert(key);
            }
        }
        // Check if need to remove existing keyINode from children inodes
        auto it = this->children.begin();
        while(it != this->children.end()) {
            std::string name = cur_pathname + "/" + (*it)->display_name;
            if((*it)->type == INodeType::KEY && (std::find(reply.begin(), reply.end(), name) == reply.end())) {
                key_children.erase(key_children.find(name));
                it = this->children.erase(it);

            } else {
                ++it;
            }
        }
    }

    virtual std::map<std::string, fuse_ino_t> get_dir_entries() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        check_update();
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return FuseClientINode::get_dir_entries();
    }

private:
    virtual void update_contents() override {
        update_objpINodes();
        update_keyINodes();
    }
};

class ObjectPoolRootINode : public ObjectPoolPathINode {
public:
    ObjectPoolRootINode(ServiceClientAPI& _capi, fuse_ino_t pino = FUSE_ROOT_ID) : ObjectPoolPathINode(pino, _capi) {
        this->display_name = "ObjectPools";
        this->parent = pino;
    }

    /** this function constructs the whole object pool directories at the beginning
     *  for all the object pools stored in metadata service
     */
    virtual std::map<std::string, fuse_ino_t> get_dir_entries() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        check_update();
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return FuseClientINode::get_dir_entries();
    }

private:
    virtual void update_contents() override {
        update_objpINodes();
        update_keyINodes();
    }
};

class ObjectPoolKeyINode : public FuseClientINode {
public:
    std::string key;
    std::unique_ptr<FileBytes> file_bytes;
    persistent::version_t version;
    uint64_t timestamp_us;
    persistent::version_t previous_version;
    persistent::version_t previous_version_by_key;  // previous version by key, INVALID_VERSION for the first value of the key.
    ServiceClientAPI& capi;

    ObjectPoolKeyINode(std::string k, fuse_ino_t pino, ServiceClientAPI& _capi) : key(k),
                                                                                  file_bytes(std::make_unique<FileBytes>()),
                                                                                  capi(_capi) {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        this->update_interval = 2;
        this->last_update_sec = 0;
        std::size_t pos = k.find_last_of('/');
        this->display_name = k.substr(pos + 1);
        this->parent = pino;
        this->type = INodeType::KEY;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

    virtual uint64_t read_file(FileBytes* _file_bytes) override {
        dbg_default_debug("-- READ FILE of key:[{}], [{}]entering {}.", this->key, gettid(), __func__);
        check_update();
        _file_bytes->size = this->file_bytes.get()->size;
        _file_bytes->bytes = static_cast<uint8_t*>(malloc(this->file_bytes.get()->size));
        memcpy(_file_bytes->bytes, this->file_bytes.get()->bytes, this->file_bytes.get()->size);
        dbg_default_debug("[{}]leaving {}.", gettid(), __func__);
        return 0;
    }

    virtual uint64_t get_file_size() override {
        dbg_default_debug("----GET FILE SIZE key is [{}].", this->key);
        check_update();
        return this->file_bytes.get()->size;
    }

    virtual ~ObjectPoolKeyINode() {
        dbg_default_info("[{}] entering {}.", gettid(), __func__);
        file_bytes.reset(nullptr);
        dbg_default_info("[{}] leaving {}.", gettid(), __func__);
    }

private:
    virtual void update_contents() override {
        dbg_default_debug("\n \n ----OBJP keyInode key is:[{}] - update content [{}] entering {}.", this->key, gettid(), __func__);
        Blob blob;
        auto result = capi.get<std::string>(key, CURRENT_VERSION, true);
        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            ObjectWithStringKey* access_ptr = reinterpret_cast<ObjectWithStringKey*>(&reply);
            this->version = access_ptr->version;
            this->timestamp_us = access_ptr->timestamp_us;
            this->previous_version = access_ptr->previous_version;
            this->previous_version_by_key = access_ptr->previous_version_by_key;
            blob = access_ptr->blob;
            this->file_bytes.get()->size = blob.size;
            this->file_bytes.get()->bytes = static_cast<uint8_t*>(malloc(blob.size));
            memcpy(this->file_bytes.get()->bytes, blob.bytes, blob.size);
            return;
        }
        dbg_default_trace("\n \n ----OBJP keyInode update content [{}] leaving  {}.", gettid(), __func__);
    }
};

class MetadataServiceRootINode : public FuseClientINode {
    ServiceClientAPI& capi;

public:
    MetadataServiceRootINode(ServiceClientAPI& _capi) : capi(_capi) {
        this->type = INodeType::METADATA_SERVICE;
        this->display_name = "MetadataService";
        this->parent = FUSE_ROOT_ID;
    }

    void initialize() {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        children.emplace_back(std::make_unique<ObjectPoolRootINode>(capi, reinterpret_cast<fuse_ino_t>(this)));
        this->children.back().get()->initialize();
        children.emplace_back(std::make_unique<DataPathLogicRootINode>(capi, reinterpret_cast<fuse_ino_t>(this)));
        this->children.back().get()->initialize();
    }
};

/**
 * TODO: add DLL FUSE support
 */
class DataPathLogicRootINode : public FuseClientINode {
    ServiceClientAPI& capi;

public:
    DataPathLogicRootINode(ServiceClientAPI& _capi, fuse_ino_t pino) : capi(_capi) {
        this->type = INodeType::DATAPATH_LOGIC;
        this->display_name = "DataPathLogic";
        this->parent = FUSE_ROOT_ID;
        this->parent = pino;
    }

    /** initialize */
    void initialize() {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
    }
};

/**
 * TODO: add DLL FUSE support
 */
template <typename CascadeType>
class DLLINode : public FuseClientINode {
public:
    std::string file_name;  // DLL id?
    ServiceClientAPI& capi;
    DLLINode(std::string& _filename, fuse_ino_t pino, ServiceClientAPI& _capi) : file_name(_filename), capi(_capi) {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        this->type = INodeType::DLL;
        this->display_name = std::string("dllfile") + _filename;
        this->parent = pino;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return 0;
    }

    virtual uint64_t get_file_size() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        uint64_t fsize = 0;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return fsize;
    }

    DLLINode(DLLINode&& fci) {
        this->type = std::move(fci.type);
        this->display_name = std::move(fci.display_name);
        this->parent = std::move(fci.parent);
        this->file_name = std::move(fci.file_name);
        this->capi = std::move(fci.capi);
    }

    virtual ~DLLINode() {
        dbg_default_info("[{}] entering {}.", gettid(), __func__);
        dbg_default_info("[{}] leaving {}.", gettid(), __func__);
    }
};

/**
 * The fuse filesystem context for fuse_client. This context will be used as 'userdata' on starting a fuse session.
 */
template <typename... CascadeTypes>

class FuseClientContext {
    template <typename CascadeType>
    using _CascadeTypeINode = CascadeTypeINode<CascadeType>;

private:
    /** initialization flag */
    std::atomic<bool> is_initialized;

    /** initialized time */
    struct timespec init_timestamp;

    /** The real cascade client talking to cascade servers. */
    ServiceClientAPI& capi;

    /** The inodes are stored in \a inodes. */
    mutils::KindMap<_CascadeTypeINode, CascadeTypes...> inodes;

    /** Metadata */
    RootMetaINode metadata_inode;

    /** ObjectPool */
    ObjectPoolRootINode objectpool_inode;

    /** Admin Metadata Service*/
    MetadataServiceRootINode admin_metadata_inode;

    /** fill inodes */
    void populate_inodes(const json& group_layout) {
        if(!group_layout.is_array()) {
            dbg_default_error("JSON group layout is invalid (array expected): {}.", group_layout.get<std::string>());
            throw std::runtime_error("JSON group layout is invalid.");
        }
        // populate from the second layout, since the first one is for metadata service
        do_populate_inodes<CascadeTypes...>(group_layout, 1);
    }

    template <typename Type>
    void do_populate_inodes(const json& group_layout, int type_idx) {
        inodes.template get<Type>().initialize(group_layout[type_idx], this->capi);
    }
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void do_populate_inodes(const json& group_layout, int type_idx) {
        this->do_populate_inodes<FirstType>(group_layout, type_idx);
        this->do_populate_inodes<SecondType, RestTypes...>(group_layout, type_idx + 1);
    }

public:
    FuseClientContext() : capi(ServiceClientAPI::get_service_client()),
                          metadata_inode(capi),
                          objectpool_inode(capi),
                          admin_metadata_inode(capi) {}
    /** initialize */
    void initialize(const json& group_layout) {
        dbg_default_trace("[{}]entering {} .", gettid(), __func__);
        populate_inodes(group_layout);
        this->admin_metadata_inode.initialize();
        clock_gettime(CLOCK_REALTIME, &this->init_timestamp);
        this->is_initialized.store(true);
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

    /** read directory entries by ino */
    std::map<std::string, fuse_ino_t> get_dir_entries(fuse_ino_t ino) {
        dbg_default_trace("[{}]entering {} with ino ={:x}.", gettid(), __func__, ino);
        std::map<std::string, fuse_ino_t> ret_map;
        if(ino == FUSE_ROOT_ID) {
            this->inodes.for_each(
                    [&ret_map](auto k, auto& v) {
                        // CAUTION: only works up to 64bit virtual address CPU architectures.
                        ret_map.emplace(v.display_name, reinterpret_cast<fuse_ino_t>(&v));
                    });
            ret_map.emplace(metadata_inode.display_name, reinterpret_cast<fuse_ino_t>(&this->metadata_inode));
            ret_map.emplace(objectpool_inode.display_name, reinterpret_cast<fuse_ino_t>(&this->objectpool_inode));
            ret_map.emplace(admin_metadata_inode.display_name, reinterpret_cast<fuse_ino_t>(&this->admin_metadata_inode));
        } else {
            FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(ino);
            ret_map = pfci->get_dir_entries();  // RVO
        }
        dbg_default_trace(" [{}]leaving {}.", gettid(), __func__);
        return ret_map;
    }

    /** fill stbuf features
     * @param stbuf     This structure is filled according to its st_ino value.
     * @return          A timeout for the filled values.
     *
     */
    double fill_stbuf_by_ino(struct stat& stbuf) {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        double timeout_sec = 1.0;  // TO_FOREVER;
        // 1 - common attributes
        stbuf.st_dev = FUSE_CLIENT_DEV_ID;
        stbuf.st_nlink = 1;
        stbuf.st_uid = getuid();
        stbuf.st_gid = getgid();
        stbuf.st_atim = this->init_timestamp;
        stbuf.st_mtim = this->init_timestamp;
        stbuf.st_ctim = this->init_timestamp;
        // 2 - special attributes
        if(stbuf.st_ino == FUSE_ROOT_ID) {
            stbuf.st_mode = S_IFDIR | 0755;
            stbuf.st_size = FUSE_CLIENT_BLK_SIZE;
            stbuf.st_blocks = 1;
            stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
        } else {
            FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(stbuf.st_ino);
            switch(pfci->type) {
                case INodeType::SITE:
                    // TODO:
                    break;
                case INodeType::CASCADE_TYPE:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::METADATA_SERVICE:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::SUBGROUP:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::SHARD:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::KEY:
                    stbuf.st_mode = S_IFREG | 0444;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::META:
                    stbuf.st_mode = S_IFREG | 0444;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::OBJECTPOOL_PATH:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::DATAPATH_LOGIC:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                case INodeType::DLL:
                    stbuf.st_mode = S_IFDIR | 0755;
                    stbuf.st_size = pfci->get_file_size();
                    stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1) / FUSE_CLIENT_BLK_SIZE;
                    stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                    break;
                default:;
            }
        }
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return timeout_sec;
    }

    /**
     * open a file.
     * @param ino       inode
     * @param fi        file structure shared among processes opening this file.
     * @return          error code. 0 for success.
     */
    int open_file(fuse_ino_t ino, struct fuse_file_info* fi) {
        dbg_default_trace("[{}]entering {} with ino={:x}.", gettid(), __func__, ino);
        FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(ino);
        if(pfci->type != INodeType::KEY && pfci->type != INodeType::META) {
            return EISDIR;
        }
        FileBytes* fb = new FileBytes();
        pfci->read_file(fb);
        fi->fh = reinterpret_cast<uint64_t>(fb);
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return 0;
    }

    /**
     * close a file.
     * @param ino       inode
     * @param fi        file structure shared among processes opening this file.
     * @return          error code. 0 for success.
     */
    int close_file(fuse_ino_t ino, struct fuse_file_info* fi) {
        dbg_default_trace("[{}]entering {} with ino={:x}.", gettid(), __func__, ino);
        void* pfb = reinterpret_cast<void*>(fi->fh);
        if(pfb != nullptr) {
            delete static_cast<FileBytes*>(pfb);
        }
        return 0;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }
};

}  // namespace cascade
}  // namespace derecho
