#pragma once
#include <memory>
#include <atomic>
#include <mutex>
#include <string>
#include <mutils-containers/KindMap.hpp>
#include <cascade/service_client_api.hpp>
#include <fuse3/fuse_lowlevel.h>

using namespace derecho::cascade;

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
class CascadeTypeINode : public FuseClientINode {
public:
    CascadeTypeINode() {
        this->type = INodeType::CASCADE_TYPE;
        this->display_name = typeid(CascadeType).name();
    }
    //TODO: fill children using capi_ptr. Check how to get view from Derecho ??? 
};

/**
 * The fuse filesystem context for fuse_client. This context will be used as 'userdata' on starting a fuse session.
 */
template <typename... CascadeTypes>
class FuseClientContext {
private:
    /** initialization flag */
    std::atomic<bool> is_initialized;

    /** The real cascade client talking to cascade servers. */
    std::unique_ptr<ServiceClient<CascadeTypes...>> capi_ptr;

    /** The inodes are stored in \a inodes. */
    mutils::KindMap<CascadeTypeINode,CascadeTypes...> inodes;

    /** mutex gaurding the inodes */
    std::shared_mutex inodes_mutex;

    /** fill inodes
    template <typename Type>
    void populate_inodes() {
        inodes.template get<Type>().emplace();
    }
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void populate_inodes() {
        this->populate_inodes<FirstType>();
        this->populate_inodes<SecondType, RestTypes...>();
    }
    */

public:
    /** initialize */
    void initialize() {
        capi_ptr = std::make_unique<ServiceClient<CascadeTypes...>>();
        this->is_initialized.store(true);
        // populate_inodes<CascadeTypes...>();
    }

    /** read root directory */
    std::map<std::string,fuse_ino_t> get_root_dir() {
        std::map<std::string, fuse_ino_t> ret_map;
        std::shared_lock rlck(this->inodes_mutex);
        this->inodes.for_each(
            [&ret_map](auto k,auto& v){
                // CAUTION: only works up to 64bit virtual address CPU architectures.
                ret_map.emplace(v.display_name,reinterpret_cast<fuse_ino_t>(&v));
            });
        return ret_map;
    }
};
