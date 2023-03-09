#pragma once

#define FUSE_USE_VERSION 34

#include <cascade/object_pool_metadata.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/utils/logger.hpp>
#include <fuse3/fuse.h>
#include <memory>
#include <mutils-containers/KindMap.hpp>
#include <nlohmann/json.hpp>

#include "path_tree.hpp"

namespace fs = std::filesystem;

std::shared_ptr<spdlog::logger> DL;

enum NodeFlag : uint32_t {
    ROOT_DIR = 1 << 0,

    OP_PREFIX_DIR = 1 << 1,
    OP_ROOT_DIR = 1 << 2,

    KEY_DIR = 1 << 3,
    KEY_FILE = 1 << 4,

    LATEST_DIR = 1 << 5,

    METADATA_PREFIX_DIR = 1 << 6,
    METADATA_INFO_FILE = 1 << 7,

    SNAPSHOT_ROOT_DIR = 1 << 8,
    SNAPSHOT_TIME_DIR = 1 << 9,
};

const uint32_t FILE_FLAG = KEY_FILE | METADATA_INFO_FILE;
const uint32_t DIR_FLAG = ~FILE_FLAG;
const uint32_t OP_FLAG = OP_PREFIX_DIR | OP_ROOT_DIR | KEY_DIR | KEY_FILE;

struct NodeData {
    NodeFlag flag;
    // timestamp in microsec
    uint64_t timestamp;
    std::vector<uint8_t> bytes;

    NodeData(const NodeFlag flag) : flag(flag), timestamp(0) {
    }
};

namespace derecho {
namespace cascade {

struct FuseClientContext {
    using Node = PathTree<NodeData>;

    std::unique_ptr<Node> root;

    // object pools are not versioned? :(
    const fs::path METADATA_PATH = "/.cascade";
    const fs::path SNAPSHOT_PATH = "/snapshot";
    const fs::path LATEST_PATH = "/latest";
    const fs::path ROOT = "/";
    ServiceClientAPI& capi;

    persistent::version_t max_ver;

    std::set<fs::path> local_latest_dirs;
    std::set<persistent::version_t> snapshots;

    time_t update_interval;
    time_t last_update_sec;

    FuseClientContext() : capi(ServiceClientAPI::get_service_client()) {
        DL = LoggerFactory::createLogger("fuse_client", spdlog::level::from_str(derecho::getConfString(CONF_LOGGER_DEFAULT_LOG_LEVEL)));
        DL->set_pattern("[%T][%n][%^%l%$] %v");

        max_ver = 0;

        update_interval = 15;
        last_update_sec = 0;

        root = std::make_unique<Node>(ROOT, NodeData(ROOT_DIR));
        root->set(SNAPSHOT_PATH, NodeData(SNAPSHOT_ROOT_DIR), NodeData(SNAPSHOT_ROOT_DIR));
        reset_latest();

        update_object_pools();
    }

    /*  --- pathtree related logic ---  */

    void reset_latest() {
        auto latest = root->get(LATEST_PATH);
        if(latest != nullptr) {
            latest->data = NodeData(LATEST_DIR);
            latest->children.clear();
        } else {
            root->set(LATEST_PATH, NodeData(LATEST_DIR), NodeData(LATEST_DIR));
        }
    }

    Node* add_op_info(const fs::path& path, const std::string& contents) {
        auto node = root->set(path, NodeData(METADATA_PREFIX_DIR),
                              NodeData(METADATA_INFO_FILE));
        if(node == nullptr) {
            return nullptr;
        }
        node->data.bytes = std::vector<uint8_t>(contents.begin(), contents.end());
        return node;
    }

    Node* add_snapshot_time(const fs::path& path) {
        return root->set(path, NodeData(SNAPSHOT_ROOT_DIR), NodeData(SNAPSHOT_TIME_DIR));
    }

    Node* add_op_root(const fs::path& path) {
        return root->set(path, NodeData(OP_PREFIX_DIR), NodeData(OP_ROOT_DIR));
    }

    Node* add_op_key(const fs::path& path) {
        // invariant: assumes op_root already exists
        return root->set(path, NodeData(KEY_DIR), NodeData(KEY_FILE));
    }

    Node* add_op_key_dir(const fs::path& path) {
        // invariant: assumes op_root already exists
        return root->set(path, NodeData(KEY_DIR), NodeData(KEY_DIR));
    }

    Node* object_pool_root(Node* node) {
        if(node == nullptr) {
            return nullptr;
        }
        if(node->parent == nullptr || node->data.flag & OP_ROOT_DIR) {
            return node;
        }
        if(node->data.flag & (KEY_DIR | KEY_FILE)) {
            return object_pool_root(node->parent);
        }
        return nullptr;
    }

    Node* nearest_object_pool_root(const fs::path& path) {
        auto node = root->get_while_valid(path);
        return object_pool_root(node);
    }

    persistent::version_t is_snapshot(const fs::path& path) {
        if(path.parent_path() != SNAPSHOT_PATH) {
            return CURRENT_VERSION;
        }
        try {
            return std::stoull(path.filename());
        } catch(std::invalid_argument const& ex) {
            return CURRENT_VERSION;
        } catch(std::out_of_range const& ex) {
            return CURRENT_VERSION;
        }
    }

    /*  --- capi related logic ---  */

    bool should_update() {
        if(time(0) > last_update_sec + update_interval) {
            return true;
        }
        return false;
    }

    std::string path_while_op(const Node* node) const {
        std::vector<std::string> parts;
        for(; node != nullptr && (node->data.flag & OP_FLAG); node = node->parent) {
            parts.push_back(node->label);
        }
        std::string res;
        for(auto it = parts.rbegin(); it != parts.rend(); ++it) {
            res += "/" + *it;
        }

        return res;
    }

    int put_to_capi(const Node* node) {
        // invariant: node is file
        ObjectWithStringKey obj;
        obj.key = path_while_op(node);
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

    void fill_at(const fs::path& prefix, persistent::version_t ver) {
        // TODO no version for list object pools
        auto str_paths = capi.list_object_pools(false, true);

        auto meta_path = prefix;
        meta_path += METADATA_PATH;
        root->set(meta_path,
                  NodeData(METADATA_PREFIX_DIR), NodeData(METADATA_PREFIX_DIR));
        for(const std::string& op_root : str_paths) {
            auto op_root_path = prefix;
            op_root_path += op_root;
            auto op_root_node = add_op_root(op_root_path);
            if(op_root_node == nullptr) {
                continue;
            }

            auto opm = capi.find_object_pool(op_root);
            json j{{"valid", opm.is_valid()},
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
            auto op_root_meta_path = prefix;
            op_root_meta_path += METADATA_PATH;
            op_root_meta_path += op_root;
            add_op_info(op_root_meta_path, j.dump(2));

            auto keys = get_keys(op_root, ver);
            std::sort(keys.begin(), keys.end(), std::greater<>());
            // sort removes files colliding with directory
            for(const auto& k : keys) {
                auto key_path = prefix;
                key_path += k;
                auto node = add_op_key(key_path);
                if(node == nullptr) {
                    dbg_info(DL, "did not add {}", key_path);
                } else {
                    get_contents(node, k, ver);
                    // dbg_info(DL, "file: {}", std::quoted(reinterpret_cast<const char*>(node->data.bytes.data())));
                }
            }
        }
    }

    void add_snapshot(persistent::version_t ver) {
        auto snapshot = SNAPSHOT_PATH;
        snapshot += "/" + std::to_string(ver);
        auto res = add_snapshot_time(snapshot);
        dbg_info(DL, "adding {}", snapshot);
        if(res != nullptr) {
            dbg_info(DL, "ok");
            fill_at(snapshot, ver);
            dbg_info(DL, "filled");
        }
    }

    void update_object_pools() {
        // TODO use old cached data
        reset_latest();

        fill_at(LATEST_PATH, CURRENT_VERSION);

        for(auto it = local_latest_dirs.begin(); it != local_latest_dirs.end();) {
            if(nearest_object_pool_root(*it) == nullptr) {
                // TODO verify for op_root deleted
                it = local_latest_dirs.erase(it);
            } else {
                add_op_key_dir(*it);
                ++it;
            }
        }

        dbg_info(DL, "updating contents\n{}", string());

        last_update_sec = time(0);
    }

    int get_stat(Node* node, struct stat* stbuf) {
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

        if(node->data.flag & DIR_FLAG) {
            if(node->data.flag & OP_PREFIX_DIR) {
                stbuf->st_mode = S_IFDIR | 0555;
            } else {
                stbuf->st_mode = S_IFDIR | 0755;
            }
            stbuf->st_nlink = 2;  // TODO calculate properly
            for(const auto& [_, v] : node->children) {
                stbuf->st_nlink += v->data.flag & DIR_FLAG;
            }
        } else {
            // TODO somehow even when 0444, can still write ???
            stbuf->st_mode = S_IFREG | (node->data.flag & KEY_FILE ? 0744 : 0444);
            stbuf->st_size = node->data.bytes.size();
        }

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
        return 0;
    }

    std::string string() {
        std::stringstream ss;
        root->print(100, ss);
        return ss.str();
    }

    void get_contents(Node* node, const std::string& path, persistent::version_t ver) {
        // persistent::version_t version = CURRENT_VERSION;
        auto result = capi.get(path, ver, true);
        // TODO only get file contents on open

        for(auto& reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if(ver == CURRENT_VERSION) {
                max_ver = std::max(max_ver, reply.version);
            }
            Blob blob = reply.blob;
            std::vector<uint8_t> bytes(blob.bytes, blob.bytes + blob.size);
            node->data.bytes = bytes;
            node->data.timestamp = reply.timestamp_us;
            return;
        }
    }

    std::vector<std::string> get_keys(const std::string& path, persistent::version_t ver) {
        // persistent::version_t version = CURRENT_VERSION;
        auto future_result = capi.list_keys(ver, true, path);
        auto reply = capi.wait_list_keys(future_result);
        // TODO remove keys that collide with directory (key is a prefix of another key)
        return reply;
    }

    // make a trash folder? (move on delete)

    Node* get(const std::string& path) {
        if(should_update()) {
            update_object_pools();
            // split between update object pool list and update keys
        }
        return root->get(path);
    }
    // TODO use object pool root meta file to edit version # and such?

    // TODO cascade metaservice api. need:
    // - get children given path. shouldnt be hard considering op_list_keys works from subdir of op root
    // - get file metadata WITHOUT getting file contents (file size, modification time)
};

}  // namespace cascade
}  // namespace derecho
