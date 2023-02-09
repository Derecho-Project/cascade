#pragma once

#define FUSE_USE_VERSION 34

#include <cascade/object_pool_metadata.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/utils/logger.hpp>
#include <fuse3/fuse.h>
#include <mutils-containers/KindMap.hpp>
#include <nlohmann/json.hpp>

#include "path_tree.hpp"

namespace fs = std::filesystem;

std::shared_ptr<spdlog::logger> DL;

enum NodeFlag {
    OP_PREFIX_DIR = 1 << 0,
    OP_ROOT_DIR = 1 << 1,
    OP_KEY_DIR = 1 << 2,
    OP_KEY = 1 << 3,
    OP_INFO = 1 << 4,
};

const int OP_DIR = OP_PREFIX_DIR | OP_ROOT_DIR | OP_KEY_DIR;

struct NodeData {
    NodeFlag flag;
    // timestamp in microsec
    uint64_t timestamp;
    std::vector<uint8_t> bytes;

    NodeData(const NodeFlag flag) : flag(flag), timestamp(0) {
    }
};

static const char* ROOT = "/";

struct FSTree {
    using Node = PathTree<NodeData>;

    Node* root;

    FSTree() {
        root = new Node(ROOT, NodeData(OP_PREFIX_DIR));
    }

    ~FSTree() { delete root; }

    Node* add_op_root(const fs::path& path) {
        // TODO add info
        return root->set(path, NodeData(OP_PREFIX_DIR), NodeData(OP_ROOT_DIR));
    }

    Node* add_op_info(const fs::path& path, const std::string& contents) {
        auto node = root->set(path, NodeData(OP_PREFIX_DIR), NodeData(OP_INFO));
        if(node == nullptr) {
            return nullptr;
        }
        node->data.bytes = std::vector<uint8_t>(contents.begin(), contents.end());
        return node;
    }

    Node* add_op_key(const fs::path& path) {
        // invariant: assumes op_root already exists
        return root->set(path, NodeData(OP_KEY_DIR), NodeData(OP_KEY));
    }

    Node* add_op_key_dir(const fs::path& path) {
        // invariant: assumes op_root already exists
        return root->set(path, NodeData(OP_KEY_DIR), NodeData(OP_KEY_DIR));
    }

    void new_op_paths(const std::vector<fs::path>& paths) {
        auto old_root = root;
        root = new Node(ROOT, NodeData(OP_PREFIX_DIR));
        for(const auto& path : paths) {
            auto op_root = add_op_root(path);
            if(op_root == nullptr) {
                continue;
                // TODO errors
            }
            auto old_op_root = old_root->extract(path);
            if(old_op_root != nullptr) {
                Node::replace(op_root, old_op_root);
            }
        }
        delete old_root;
    }

    static Node* object_pool_root(Node* node) {
        if(node == nullptr || node->data.flag & OP_PREFIX_DIR) {
            return nullptr;
        }
        if(node->parent == nullptr || node->data.flag & OP_ROOT_DIR) {
            return node;
        }
        return object_pool_root(node->parent);
    }

    Node* object_pool_root(const fs::path& path) {
        auto node = root->get_while_valid(path);
        return object_pool_root(node);
    }
};

namespace derecho {
namespace cascade {

struct FuseClientContext {
    const std::string INFO_EXT = ".cacade";
    ServiceClientAPI& capi;

    persistent::version_t ver;
    // TODO
    persistent::version_t max_ver;
    bool latest;

    FSTree* tree;

    std::set<fs::path> local_dirs;

    time_t update_interval;
    time_t last_update_sec;

    FuseClientContext() : capi(ServiceClientAPI::get_service_client()) {
        DL = LoggerFactory::createLogger("fuse_client", spdlog::level::from_str(derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_LEVEL)));
        DL->set_pattern("[%T][%n][%^%l%$] %v");

        // clock_gettime(CLOCK_REALTIME, &GLOBAL_INIT_TIMESTAMP);

        ver = 0;
        latest = true;

        update_interval = 15;
        last_update_sec = 0;

        tree = new FSTree();

        update_object_pools();
    }

    ~FuseClientContext() {
        try {
            dbg_debug(DL, "deleting tree");
            delete tree;
        } catch(...) {
            dbg_error(DL, "could not delete tree");
        }
    }

    bool should_update() {
        if(time(0) > last_update_sec + update_interval) {
            return true;
        }
        return false;
    }

    int put(const FSTree::Node* node) {
        // invariant: node is file
        ObjectWithStringKey obj;
        obj.key = node->absolute_path();
        obj.previous_version = INVALID_VERSION;
        obj.previous_version_by_key = INVALID_VERSION;
        obj.blob = Blob(node->data.bytes.data(), node->data.bytes.size(), true);
        // TODO verify emplaced avoids blob deleting data :(

        auto result = capi.put(obj);
        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            dbg_info(DL, "node({}) replied with version:{},ts_us:{}",
                     reply_future.first, std::get<0>(reply), std::get<1>(reply));
        }
        // TODO check for error

        return 0;
    }

    void update_object_pools() {
        // TODO avoid re generating op keys? ... by not resetting tree
        FSTree* old_tree = tree;
        delete old_tree;
        tree = new FSTree();

        // TODO no version for list object pools
        auto reply = capi.list_object_pools(true);
        std::vector<fs::path> paths(reply.size());
        for(size_t i = 0; i < reply.size(); ++i) {
            paths[i] = fs::path(reply[i]);
        }
        tree->new_op_paths(paths);

        for(size_t i = 0; i < reply.size(); ++i) {
            auto opm = capi.find_object_pool(reply[i]);
            json j{
                    {"valid", opm.is_valid()},
                    {"null", opm.is_null()}};
            if(opm.is_valid() && !opm.is_null()) {
                j.emplace("pathname", opm.pathname);
                j.emplace("version", opm.version);
                j.emplace("timestamp_us", opm.timestamp_us);
                j.emplace("previous_version", opm.previous_version);
                j.emplace("previous_version_by_key", opm.previous_version_by_key);
                j.emplace("subgroup_type", std::to_string(opm.subgroup_type_index) + "-->" + DefaultObjectPoolMetadataType::subgroup_type_order[opm.subgroup_type_index].name());
                j.emplace("subgroup_index", opm.subgroup_index);
                j.emplace("sharding_policy", std::to_string(opm.sharding_policy));
                j.emplace("deleted", opm.deleted);
            }
            tree->add_op_info(reply[i] + INFO_EXT, j.dump(2));
        }

        for(const auto& op_root : paths) {
            if(tree->root->get(op_root) != nullptr) {
                auto keys = get_keys(op_root);
                std::sort(keys.begin(), keys.end(), std::greater<>());
                for(const auto& k : keys) {
                    // TODO verify remove file colliding with directory here
                    auto key = tree->add_op_key(k);
                    if(key == nullptr) {
                        dbg_info(DL, "did not add {}", k);
                    } else {
                        get_contents(key, k);
                        // dbg_info(DL, "file: {}", std::quoted(reinterpret_cast<const char*>(key->data.bytes.data())));
                    }
                }
            }
        }

        for(auto it = local_dirs.begin(); it != local_dirs.end();) {
            if(tree->object_pool_root(*it) == nullptr) {
                // TODO verify for op_root deleted
                it = local_dirs.erase(it);
            } else {
                tree->add_op_key_dir(*it);
                ++it;
            }
        }

        dbg_info(DL, "updating contents\n{}", string());

        last_update_sec = time(0);
    }

    int get_stat(FSTree::Node* node, struct stat* stbuf) {
        if(node == nullptr) {
            return -ENOENT;
        }
        // not needed: st_dev, st_blksize, st_ino (unless use_ino mount option)
        // TODO maintain stbuf in data?
        stbuf->st_nlink = 1;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_atim = timespec{last_update_sec, 0};
        // TODO merge timestamp for dirs, update during set
        int64_t sec = node->data.timestamp / 1'000'000;
        int64_t nano = (node->data.timestamp % 1'000'000) * 1000;
        stbuf->st_mtim = timespec{sec, nano};
        stbuf->st_ctim = stbuf->st_mtim;
        // - at prefix dir location add .info file ???

        if(node->data.flag & OP_DIR) {
            if(node->data.flag & OP_PREFIX_DIR) {
                stbuf->st_mode = S_IFDIR | 0555;
            } else {
                stbuf->st_mode = S_IFDIR | 0755;
            }
            stbuf->st_nlink = 2;  // TODO calculate properly
            for(const auto& [_, v] : node->children) {
                stbuf->st_nlink += v->data.flag & OP_DIR;
            }
        } else {
            // TODO somehow even when 0444, can still write ???
            stbuf->st_mode = S_IFREG | (node->data.flag & OP_KEY ? 0744 : 0444);
            stbuf->st_size = node->data.bytes.size();
        }
        return 0;
    }

    std::string string() {
        std::stringstream ss;
        tree->root->print(100, ss);
        return ss.str();
    }

    void get_contents(FSTree::Node* node, const std::string& path) {
        // persistent::version_t version = CURRENT_VERSION;
        auto result = capi.get(path, latest ? CURRENT_VERSION : ver, true);
        // TODO only get file contents on open

        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if(latest) {
                ver = std::max(ver, reply.version);
            }
            Blob blob = reply.blob;
            std::vector<uint8_t> bytes(blob.bytes, blob.bytes + blob.size);
            node->data.bytes = bytes;
            node->data.timestamp = reply.timestamp_us;
            return;
        }
    }

    std::vector<std::string> get_keys(const std::string& path) {
        // persistent::version_t version = CURRENT_VERSION;
        auto future_result = capi.list_keys(latest ? CURRENT_VERSION : ver, true, path);
        auto reply = capi.wait_list_keys(future_result);
        // TODO remove keys that collide with directory (key is a prefix of another key)
        return reply;
    }

    // make a trash folder? (move on delete)

    FSTree::Node* get(const std::string& path) {
        if(should_update()) {
            update_object_pools();
            // split between update object pool list and update keys
        }
        return tree->root->get(path);
    }
    // TODO use object pool root meta file to edit version # and such?

    // TODO cascade metaservice api. need:
    // - get children given path. shouldnt be hard considering op_list_keys works from subdir of op root
    // - get file metadata WITHOUT getting file contents (file size, modification time)

    /*
    stat

    */
    // dev_t st_dev;         /* ID of device containing file */
    // ino_t st_ino;         /* Inode number */
    // mode_t st_mode;       /* File type and mode */
    // nlink_t st_nlink;     /* Number of hard links */
    // uid_t st_uid;         /* User ID of owner */
    // gid_t st_gid;         /* Group ID of owner */
    // dev_t st_rdev;        /* Device ID (if special file) */
    // off_t st_size;        /* Total size, in bytes */
    // blksize_t st_blksize; /* Block size for filesystem I/O */
    // blkcnt_t st_blocks;   /* Number of 512B blocks allocated */

    // struct timespec st_atim; /* Time of last access */
    // struct timespec st_mtim; /* Time of last modification */
    // struct timespec st_ctim; /* Time of last status change */
};

}  // namespace cascade
}  // namespace derecho