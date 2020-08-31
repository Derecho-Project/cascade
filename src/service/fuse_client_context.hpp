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

#define TO_FOREVER              (std::numeric_limits<double>::max())

typedef enum {
    SITE = 0,
    CASCADE_TYPE,
    SUBGROUP,
    SHARD,
    KEY,
} INodeType;

class FuseClientINode {
public:
    INodeType type;
    std::string display_name;
    std::vector<std::unique_ptr<FuseClientINode>> children;
};

template <typename CascadeType>
class SubgroupINode;

template <typename CascadeType>
class ShardINode;

template <typename CascadeType>
class CascadeTypeINode : public FuseClientINode {
public:
    CascadeTypeINode() {
        this->type = INodeType::CASCADE_TYPE;
        this->display_name = typeid(CascadeType).name();
    }

    /** initialize */
    void initialize(const json& group_layout) {
        this->display_name = group_layout["type_alias"];

        uint32_t sidx=0;
        for (auto subgroup_it:group_layout[JSON_CONF_LAYOUT]) {
            // TODO: add metadata file/key file for each folder here.
            children.emplace_back(std::make_unique<SubgroupINode<CascadeType>>(sidx));
            size_t num_shards = subgroup_it[MIN_NODES_BY_SHARD].size();
            for (uint32_t shidx = 0; shidx < num_shards; shidx ++) {
                this->children[sidx]->children.emplace_back(std::make_unique<ShardINode<CascadeType>>(shidx));
            }
            sidx ++;
        }
    }
};

template <typename CascadeType>
class SubgroupINode : public FuseClientINode {
public:
    const uint32_t subgroup_index;

    SubgroupINode (uint32_t sidx) : subgroup_index(sidx) {
        this->type = INodeType::SUBGROUP;
        this->display_name = "subgroup-" + std::to_string(sidx);
    }
};

template <typename CascadeType>
class ShardINode : public FuseClientINode {
public:
    const uint32_t shard_index;

    ShardINode (uint32_t shidx) : shard_index (shidx) {
        this->type = INodeType::SHARD;
        this->display_name = "shard-" + std::to_string(shidx);
    }
};

/**
 * The fuse filesystem context for fuse_client. This context will be used as 'userdata' on starting a fuse session.
 */
template <typename... CascadeTypes>
class FuseClientContext {
private:
    /** initialization flag */
    std::atomic<bool> is_initialized;

    /** initialized time */
    struct timespec init_timestamp;

    /** The real cascade client talking to cascade servers. */
    std::unique_ptr<ServiceClient<CascadeTypes...>> capi_ptr;

    /** The inodes are stored in \a inodes. */
    mutils::KindMap<CascadeTypeINode,CascadeTypes...> inodes;

    /** mutex gaurding the inodes */
    std::shared_mutex inodes_mutex;

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
        inodes.template get<Type>().initialize(group_layout[type_idx]);
    }
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void do_populate_inodes(const json& group_layout, int type_idx) {
        this->do_populate_inodes<FirstType>(group_layout, type_idx);
        this->do_populate_inodes<SecondType, RestTypes...>(group_layout, type_idx+1);
    }

public:
    /** initialize */
    void initialize(const json& group_layout) {
        this->capi_ptr = std::make_unique<ServiceClient<CascadeTypes...>>();
        populate_inodes(group_layout);
        clock_gettime(CLOCK_REALTIME,&this->init_timestamp);
        this->is_initialized.store(true);
    }

    /** read directory entries by ino */
    std::map<std::string,fuse_ino_t> get_dir_entries(fuse_ino_t ino) {
        dbg_default_debug("get_dir_entries({:x}).",ino);
        std::map<std::string, fuse_ino_t> ret_map;
        std::shared_lock rlck(this->inodes_mutex);
        if (ino == FUSE_ROOT_ID) {
            this->inodes.for_each(
                [&ret_map](auto k,auto& v){
                    // CAUTION: only works up to 64bit virtual address CPU architectures.
                    ret_map.emplace(v.display_name,reinterpret_cast<fuse_ino_t>(&v));
                });
        } else {
            FuseClientINode* pfci = reinterpret_cast<FuseClientINode*>(ino);
            switch (pfci->type) {
            case SITE:
                //TODO:
                dbg_default_debug("Skipping unimplemented inode type:{}.", pfci->type);
                break;
            case CASCADE_TYPE:
            case SUBGROUP:
                for (auto& child: pfci->children) {
                    ret_map.emplace(child->display_name,reinterpret_cast<fuse_ino_t>(child.get()));
                }
                break;
            case SHARD:
                //TODO:
                dbg_default_error("Skipping unimplemented inode type:{}.", pfci->type);
                break;
            default:
                dbg_default_error("Skipping unknown inode type:{}.", pfci->type);
            }
        }
        return ret_map;
    }

    /** fill stbuf features
     * @param stbuf     This structure is filled according to its st_ino value.
     * @return          A timeout for the filled values.
     *
     */
    double fill_stbuf_by_ino(struct stat& stbuf) {
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
                stbuf.st_size = sizeof(*pfci);
                stbuf.st_blocks = (sizeof(*pfci)+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::SUBGROUP:
                stbuf.st_mode = S_IFDIR + 0755;
                stbuf.st_size = sizeof(*pfci);
                stbuf.st_blocks = (sizeof(*pfci)+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::SHARD:
                stbuf.st_mode = S_IFDIR + 0755;
                stbuf.st_size = sizeof(*pfci);
                stbuf.st_blocks = (sizeof(*pfci)+FUSE_CLIENT_BLK_SIZE-1)/FUSE_CLIENT_BLK_SIZE;
                stbuf.st_blksize = FUSE_CLIENT_BLK_SIZE;
                break;
            case INodeType::KEY:
                // TODO:
                break;
            default:
                ;
            }
        }
        return timeout_sec;
    }
};

} // namespace cascade
} // namespace derecho
