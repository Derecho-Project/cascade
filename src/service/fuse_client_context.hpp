#pragma once
#include <memory>
#include <atomic>
#include <mutex>
#include <string>
#include <limits>
#include <mutils-containers/KindMap.hpp>
#include <cascade/service_client_api.hpp>
#include <fuse3/fuse_lowlevel.h>
#include <nlohmann/json.hpp>
#include <derecho/utils/logger.hpp>
#include <time.h>


namespace derecho {
namespace cascade{

using json = nlohmann::json;
#define FUSE_CLIENT_DEV_ID      (0xCA7CADE)
#define FUSE_CLIENT_BLK_SIZE    (4096)

#define META_FILE_NAME          ".cascade"

#define TO_FOREVER              (std::numeric_limits<double>::max())

typedef enum {
    SITE = 0,
    CASCADE_TYPE,
    SUBGROUP,
    SHARD,
    KEY,
    META,
} INodeType;

class FileBytes {
public:
    size_t size;
    char* bytes;
    FileBytes():size(0),bytes(nullptr){}
    FileBytes(size_t s):size(s) {
        bytes = nullptr;
        if (s > 0) {
            bytes = (char*)malloc(s);
        }
    }
    virtual ~FileBytes() {
        if (bytes){
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

    /**
     * get directory entries. This is the default implementation.
     * Override it as required.
     */
    virtual std::map<std::string,fuse_ino_t> get_dir_entries() {
        std::map<std::string,fuse_ino_t> ret_map;
        for (auto& child: this->children) {
            ret_map.emplace(child->display_name,reinterpret_cast<fuse_ino_t>(child.get()));
        }
        return ret_map;
    }

    virtual uint64_t get_file_size() {
        return sizeof(*this);
    }

    virtual uint64_t read_file(FileBytes* fb) {
        (void) fb;
        return 0;
    }
};

template <typename CascadeType, typename ServiceClientType>
class SubgroupINode;

template <typename CascadeType, typename ServiceClientType>
class ShardINode;

template <typename CascadeType, typename ServiceClientType>
class KeyINode;

template <typename ServiceClientType>
class RootMetaINode;

template <typename CascadeType, typename ServiceClientType>
class ShardMetaINode;

template <typename CascadeType, typename ServiceClientType>
class CascadeTypeINode : public FuseClientINode {
public:
    CascadeTypeINode() {
        this->type = INodeType::CASCADE_TYPE;
        this->display_name = typeid(CascadeType).name();
        this->parent = FUSE_ROOT_ID;
    }

    /** initialize */
    void initialize(const json& group_layout, std::unique_ptr<ServiceClientType>& capi_ptr) {
        this->display_name = group_layout["type_alias"];

        uint32_t sidx=0;
        for (auto subgroup_it:group_layout[JSON_CONF_LAYOUT]) {
            // TODO: add metadata file/key file for each folder here.
            children.emplace_back(std::make_unique<SubgroupINode<CascadeType, ServiceClientType>>(sidx,reinterpret_cast<fuse_ino_t>(this)));
            size_t num_shards = subgroup_it[MIN_NODES_BY_SHARD].size();
            for (uint32_t shidx = 0; shidx < num_shards; shidx ++) {
                this->children[sidx]->children.emplace_back(
                            std::make_unique<ShardINode<CascadeType, ServiceClientType>>(
                                shidx,reinterpret_cast<fuse_ino_t>(this->children[sidx].get()),capi_ptr));
            }
            sidx ++;
        }
    }
};

template <typename ServiceClientType>
class RootMetaINode : public FuseClientINode {
    const time_t update_interval;
    std::unique_ptr<ServiceClientType>& capi_ptr;
    time_t last_update_sec;
    std::string contents;
    std::shared_mutex mutex;
    /**
     * update the metadata. need write lock.
     */
    void update_contents () {
        auto members = capi_ptr->get_members();
        contents = "number of nodes in cascade service: " + std::to_string(members.size()) + ".\nnode IDs: ";
        for (auto& nid : members) {
            contents += std::to_string(nid) + ",";
        }
        contents += "\n";
    }
public:
    RootMetaINode (std::unique_ptr<ServiceClientType>& _capi_ptr) :
        update_interval (2), // membership refreshed in 2 seconds.
        capi_ptr (_capi_ptr), 
        last_update_sec(0) {
        this->type = INodeType::META;
        this->display_name = META_FILE_NAME;
    }

    virtual uint64_t get_file_size() override {
        struct timespec now;
        std::shared_lock rlck(mutex);
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > (last_update_sec + update_interval)){
            rlck.unlock();
            std::unique_lock wlck(mutex);
            clock_gettime(CLOCK_REALTIME, &now);
            if (now.tv_sec > (last_update_sec + update_interval)){
                update_contents();
            }
            return contents.size();
        } 
        return contents.size();
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        struct timespec now;
        std::shared_lock rlck(mutex);
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > (last_update_sec + update_interval)){
            rlck.unlock();
            std::unique_lock wlck(mutex);
            clock_gettime(CLOCK_REALTIME, &now);
            if (now.tv_sec > (last_update_sec + update_interval)){
                update_contents();
            }
            file_bytes->size = contents.size();
            file_bytes->bytes = strdup(contents.c_str());
        } else {
            file_bytes->size = contents.size();
            file_bytes->bytes = strdup(contents.c_str());
        }
        return 0;
    }
};

template <typename CascadeType, typename ServiceClientType>
class SubgroupINode : public FuseClientINode {
public:
    const uint32_t subgroup_index;

    SubgroupINode (uint32_t sidx, fuse_ino_t pino) : subgroup_index(sidx) {
        this->type = INodeType::SUBGROUP;
        this->display_name = "subgroup-" + std::to_string(sidx);
        this->parent = pino;
    }
};

template <typename CascadeType, typename ServiceClientType>
class ShardINode : public FuseClientINode {
public:
    const uint32_t shard_index;
    std::unique_ptr<ServiceClientType>& capi_ptr;
    std::map<typename CascadeType::KeyType, fuse_ino_t> key_to_ino;

    ShardINode (uint32_t shidx, fuse_ino_t pino, std::unique_ptr<ServiceClientType>& _capi_ptr) : 
        shard_index (shidx), capi_ptr(_capi_ptr) {
        this->type = INodeType::SHARD;
        this->display_name = "shard-" + std::to_string(shidx);
        this->parent = pino;
        SubgroupINode<CascadeType,ServiceClientType>* p_subgroup_inode = reinterpret_cast<SubgroupINode<CascadeType,ServiceClientType>*>(pino);
        this->children.emplace_back(std::make_unique<ShardMetaINode<CascadeType, ServiceClientType>>(shidx,p_subgroup_inode->subgroup_index,capi_ptr));
    }

    // TODO: rethinking about the consistency
    virtual std::map<std::string,fuse_ino_t> get_dir_entries() override {
        dbg_default_trace("[{}]entering {}.",gettid(),__func__);
        std::map<std::string,fuse_ino_t> ret_map;
        /** we always retrieve the key list for a shard inode because the data is highly dynamic */
        uint32_t subgroup_index = reinterpret_cast<SubgroupINode<CascadeType, ServiceClientType>*>(this->parent)->subgroup_index;
        auto result =  capi_ptr->template list_keys<CascadeType>(CURRENT_VERSION, subgroup_index, this->shard_index);
        for (auto& reply_future:result.get()) {
            auto reply = reply_future.second.get();
            std::unique_lock wlck(this->children_mutex);
            for (auto& key: reply) {
                if (key_to_ino.find(key) == key_to_ino.end()) {
                    this->children.emplace_back(std::make_unique<KeyINode<CascadeType, ServiceClientType>>(key,reinterpret_cast<fuse_ino_t>(this),capi_ptr));
                    key_to_ino[key] = reinterpret_cast<fuse_ino_t>(this->children.back().get());
                }
            }
        }
        dbg_default_trace("[{}]leaving {}.",gettid(),__func__);
        return FuseClientINode::get_dir_entries();
    }
};

template <typename CascadeType, typename ServiceClientType>
class ShardMetaINode : public FuseClientINode {
private:
    const uint32_t shard_index;
    const uint32_t subgroup_index;
    const time_t update_interval;
    std::unique_ptr<ServiceClientType>& capi_ptr;
    time_t last_update_sec;
    std::string contents;
    std::shared_mutex mutex;
    /**
     * update the metadata. need write lock.
     */
    void update_contents () {
        auto members = capi_ptr->template get_shard_members<CascadeType>(subgroup_index,shard_index);
        contents = "number of nodes shard: " + std::to_string(members.size()) + ".\nnode IDs: ";
        for (auto& nid : members) {
            contents += std::to_string(nid) + ",";
        }
        contents += "\n";
        auto policy = capi_ptr->template get_member_selection_policy<CascadeType>(subgroup_index,shard_index);
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
    }
public:
    ShardMetaINode (const uint32_t _shard_index, const uint32_t _subgroup_index, std::unique_ptr<ServiceClientType>& _capi_ptr) :
        shard_index(_shard_index),
        subgroup_index(_subgroup_index),
        update_interval (2), // membership refreshed in 2 seconds.
        capi_ptr (_capi_ptr), 
        last_update_sec(0) {
        this->type = INodeType::META;
        this->display_name = META_FILE_NAME;
    }

    virtual uint64_t get_file_size() override {
        struct timespec now;
        std::shared_lock rlck(mutex);
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > (last_update_sec + update_interval)){
            rlck.unlock();
            std::unique_lock wlck(mutex);
            clock_gettime(CLOCK_REALTIME, &now);
            if (now.tv_sec > (last_update_sec + update_interval)){
                update_contents();
            }
            return contents.size();
        } 
        return contents.size();
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        struct timespec now;
        std::shared_lock rlck(mutex);
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > (last_update_sec + update_interval)){
            rlck.unlock();
            std::unique_lock wlck(mutex);
            clock_gettime(CLOCK_REALTIME, &now);
            if (now.tv_sec > (last_update_sec + update_interval)){
                update_contents();
            }
            file_bytes->size = contents.size();
            file_bytes->bytes = strdup(contents.c_str());
        } else {
            file_bytes->size = contents.size();
            file_bytes->bytes = strdup(contents.c_str());
        }
        return 0;
    }
};

template <typename CascadeType, typename ServiceClientType>
class KeyINode : public FuseClientINode {
public:
    typename CascadeType::KeyType key;
    std::unique_ptr<ServiceClientType>& capi_ptr;
    KeyINode(typename CascadeType::KeyType& k, fuse_ino_t pino, std::unique_ptr<ServiceClientType>& _capi_ptr) : 
        key(k), capi_ptr(_capi_ptr) {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        this->type = INodeType::KEY;
        if constexpr (std::is_same<std::remove_cv_t<typename CascadeType::KeyType>, char*>::value ||
                      std::is_same<std::remove_cv_t<typename CascadeType::KeyType>, std::string>::value) {
            this->display_name = std::string("key") + k;
        } else if constexpr (std::is_arithmetic<std::remove_cv_t<typename CascadeType::KeyType>>::value) {
            this->display_name = std::string("key") + std::to_string(k);
        } else {
            // KeyType is required to implement to_string() for types other than type string/arithmetic.
            this->display_name = key.to_string();
        }
        this->parent = pino;
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
    }

    virtual uint64_t read_file(FileBytes* file_bytes) override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        ShardINode<CascadeType,ServiceClientType> *pino_shard = reinterpret_cast<ShardINode<CascadeType,ServiceClientType>*>(this->parent);
        SubgroupINode<CascadeType,ServiceClientType> *pino_subgroup = reinterpret_cast<SubgroupINode<CascadeType,ServiceClientType>*>(pino_shard->parent);
        auto result = capi_ptr->template get<CascadeType>(
                key,CURRENT_VERSION,pino_subgroup->subgroup_index,pino_shard->shard_index);
        for (auto& reply_future:result.get()) {
            auto reply = reply_future.second.get();
            file_bytes->size = mutils::bytes_size(reply);
            file_bytes->bytes = static_cast<char*>(malloc(file_bytes->size));
            mutils::to_bytes(reply,file_bytes->bytes);
        }
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return 0;
    }

    virtual uint64_t get_file_size() override {
        dbg_default_trace("[{}]entering {}.", gettid(), __func__);
        ShardINode<CascadeType,ServiceClientType> *pino_shard = reinterpret_cast<ShardINode<CascadeType,ServiceClientType>*>(this->parent);
        SubgroupINode<CascadeType,ServiceClientType> *pino_subgroup = reinterpret_cast<SubgroupINode<CascadeType,ServiceClientType>*>(pino_shard->parent);
        auto result = capi_ptr->template get_size<CascadeType>(
                key,CURRENT_VERSION,pino_subgroup->subgroup_index,pino_shard->shard_index);
        uint64_t fsize = 0;
        for (auto& reply_future:result.get()) {
            fsize = reply_future.second.get();
            break;
        }
        dbg_default_trace("[{}]leaving {}.", gettid(), __func__);
        return fsize;
    }

    virtual ~KeyINode() {
        dbg_default_info("[{}] entering {}.", gettid(), __func__);
        dbg_default_info("[{}] leaving {}.", gettid(), __func__);
    }
};

/**
 * The fuse filesystem context for fuse_client. This context will be used as 'userdata' on starting a fuse session.
 */
template <typename... CascadeTypes>
class FuseClientContext {
    template<typename CascadeType> using _CascadeTypeINode = CascadeTypeINode<CascadeType,ServiceClient<CascadeTypes...>>;
private:
    /** initialization flag */
    std::atomic<bool> is_initialized;

    /** initialized time */
    struct timespec init_timestamp;

    /** The real cascade client talking to cascade servers. */
    std::unique_ptr<ServiceClient<CascadeTypes...>> capi_ptr;

    /** The inodes are stored in \a inodes. */
    mutils::KindMap<_CascadeTypeINode,CascadeTypes...> inodes;

    /** Metadata */
    RootMetaINode<ServiceClient<CascadeTypes...>> metadata_inode;

    /** fill inodes */
    void populate_inodes(const json& group_layout) {
        if (!group_layout.is_array()) {
            dbg_default_error("JSON group layout is invalid (array expected): {}.", group_layout.get<std::string>());
            throw std::runtime_error("JSON group layout is invalid.");
        }
        do_populate_inodes<CascadeTypes ...>(group_layout,0);
    }

    template <typename Type>
    void do_populate_inodes(const json& group_layout, int type_idx) {
        inodes.template get<Type>().initialize(group_layout[type_idx], this->capi_ptr);
    }
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void do_populate_inodes(const json& group_layout, int type_idx) {
        this->do_populate_inodes<FirstType>(group_layout, type_idx);
        this->do_populate_inodes<SecondType, RestTypes...>(group_layout, type_idx+1);
    }

public:
    FuseClientContext() :
        capi_ptr(std::make_unique<ServiceClient<CascadeTypes...>>()),
        metadata_inode(capi_ptr){}
    /** initialize */
    void initialize(const json& group_layout) {
        dbg_default_debug("[{}]entering {} .", gettid(), __func__);
        populate_inodes(group_layout);
        clock_gettime(CLOCK_REALTIME,&this->init_timestamp);
        this->is_initialized.store(true);
        dbg_default_debug("[{}]leaving {}.", gettid(), __func__);
    }

    /** read directory entries by ino */
    std::map<std::string,fuse_ino_t> get_dir_entries(fuse_ino_t ino) {
        dbg_default_debug("[{}]entering {} with ino ={:x}.", gettid(), __func__, ino);
        std::map<std::string, fuse_ino_t> ret_map;
        if (ino == FUSE_ROOT_ID) {
            this->inodes.for_each(
                [&ret_map](auto k,auto& v){
                    // CAUTION: only works up to 64bit virtual address CPU architectures.
                    ret_map.emplace(v.display_name,reinterpret_cast<fuse_ino_t>(&v));
                });
            ret_map.emplace(metadata_inode.display_name,reinterpret_cast<fuse_ino_t>(&this->metadata_inode));
        } else {
            FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(ino);
            ret_map = pfci->get_dir_entries(); // RVO
        }
        dbg_default_debug("[{}]leaving {}.", gettid(), __func__);
        return ret_map;
    }

    /** fill stbuf features
     * @param stbuf     This structure is filled according to its st_ino value.
     * @return          A timeout for the filled values.
     *
     */
    double fill_stbuf_by_ino(struct stat& stbuf) {
        dbg_default_debug("[{}]entering {}.",gettid(),__func__);
        double timeout_sec = 1.0; //TO_FOREVER;
        // 1 - common attributes
        stbuf.st_dev = FUSE_CLIENT_DEV_ID;
        stbuf.st_nlink = 1;
        stbuf.st_uid = getuid();
        stbuf.st_gid = getgid();
        stbuf.st_atim = this->init_timestamp;
        stbuf.st_mtim = this->init_timestamp;
        stbuf.st_ctim = this->init_timestamp;
        // 2 - special attributes
        if (stbuf.st_ino == FUSE_ROOT_ID) {
            stbuf.st_mode = S_IFDIR | 0755;
            stbuf.st_size = FUSE_CLIENT_BLK_SIZE;
            stbuf.st_blocks = 1;
            stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
        } else {
            FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(stbuf.st_ino);
            switch (pfci->type) {
            case INodeType::SITE:
                // TODO:
                break;
            case INodeType::CASCADE_TYPE:
                stbuf.st_mode = S_IFDIR | 0755;
                stbuf.st_size = pfci->get_file_size();
                stbuf.st_blocks = (stbuf.st_size+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::SUBGROUP:
                stbuf.st_mode = S_IFDIR | 0755;
                stbuf.st_size = pfci->get_file_size();
                stbuf.st_blocks = (stbuf.st_size+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::SHARD:
                stbuf.st_mode = S_IFDIR | 0755;
                stbuf.st_size = pfci->get_file_size();
                stbuf.st_blocks = (stbuf.st_size+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::KEY:
            case INodeType::META:
                stbuf.st_mode = S_IFREG| 0444;
                stbuf.st_size = pfci->get_file_size();
                stbuf.st_blocks = (stbuf.st_size + FUSE_CLIENT_BLK_SIZE - 1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            default:
                ;
            }
        }
        dbg_default_debug("[{}]leaving {}.",gettid(),__func__);
        return timeout_sec;
    }

    /**
     * open a file.
     * @param ino       inode
     * @param fi        file structure shared among processes opening this file.
     * @return          error code. 0 for success.
     */
    int open_file(fuse_ino_t ino, struct fuse_file_info *fi) {
        dbg_default_debug("[{}]entering {} with ino={:x}.", gettid(), __func__, ino);
        FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(ino);
        if (pfci->type != INodeType::KEY && 
            pfci->type != INodeType::META) {
            return EISDIR;
        }
        FileBytes* fb = new FileBytes();
        pfci->read_file(fb);
        fi->fh = reinterpret_cast<uint64_t>(fb);
        dbg_default_debug("[{}]leaving {}.",gettid(),__func__);
        return 0;
    }

    /**
     * close a file.
     * @param ino       inode
     * @param fi        file structure shared among processes opening this file.
     * @return          error code. 0 for success.
     */
    int close_file(fuse_ino_t ino, struct fuse_file_info *fi) {
        dbg_default_debug("[{}]entering {} with ino={:x}.", gettid(), __func__, ino);
        void* pfb = reinterpret_cast<void*>(fi->fh);
        if (pfb!=nullptr) {
            delete static_cast<FileBytes*>(pfb);
        }
        return 0;
        dbg_default_debug("[{}]leaving {}.",gettid(),__func__);
    }
};

} // namespace cascade
} // namespace derecho
