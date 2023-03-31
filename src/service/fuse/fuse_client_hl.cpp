#define HAVE_SETXATTR

#include <cascade/service_types.hpp>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fcc_hl.hpp"
#include "fuse_client_signals.hpp"
#include <fuse3/fuse_lowlevel.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

using namespace derecho::cascade;

// TODO segfault debugging
// stat on invalid path

struct cli_options {
    const char* client_dir;
    int update_interval;
    int by_version;
};

static cli_options options = {
        .client_dir = nullptr,
        .update_interval = 15,
        .by_version = 0,
};

#define OPTION(t, p) \
    { t, offsetof(struct cli_options, p), 1 }

static const struct fuse_opt option_spec[] = {
        OPTION("--client=%s", client_dir),
        OPTION("--update-interval=%d", update_interval),
        OPTION("--by_version", by_version),
        FUSE_OPT_END};

static void show_help(const char* progname) {
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("    --update-interval=<secs>  Update-rate of file system contents (default: 15)\n"
           "    --client=<dir-path>    Client directory\n"
           "    --by_version           Snapshot by version number rather than timestamp in microseconds\n"
           "\n");
}

// ---

static FuseClientContext* fcc() {
    return static_cast<FuseClientContext*>(fuse_get_context()->private_data);
}

static void* cascade_fs_init(struct fuse_conn_info* conn,
                             struct fuse_config* cfg) {
    // TODO why read conf_layout_json_layout?
    // TODO don't like no control over derecho config

    return new FuseClientContext(options.update_interval, options.by_version);
}

static void cascade_fs_destroy(void* private_data) {
    delete static_cast<FuseClientContext*>(private_data);
}

static int cascade_fs_getattr(const char* path, struct stat* stbuf,
                              struct fuse_file_info* fi) {
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    memset(stbuf, 0, sizeof(struct stat));
    return fcc()->get_stat(node, stbuf);
}

// TODO :( invalid pointer dumped?? somehow cascade replys needs to be stored in a variable before
// result.get() ... maybe it somehow dangles?
static int cascade_fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                              off_t offset, struct fuse_file_info* fi,
                              enum fuse_readdir_flags flags) {
    // TODO offsets for very large reads
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    // TODO fill stbuf properly? (check for get_stat errors)
    // struct stat stbuf;
    // memset(&stbuf, 0, sizeof(struct stat));

    // FSTree::get_stat(node, &stbuf);
    filler(buf, ".", nullptr, 0, (fuse_fill_dir_flags)0);
    // TODO parent of root ???
    // FSTree::get_stat(node->parent, &stbuf);
    filler(buf, "..", nullptr, 0, (fuse_fill_dir_flags)0);
    for(const auto& [k, v] : node->children) {
        if(filler(buf, k.c_str(), nullptr, 0, (fuse_fill_dir_flags)0)) {
            break;
        }
        // FSTree::get_stat(v, &stbuf);
    }

    return 0;
}

static int cascade_fs_open(const char* path, struct fuse_file_info* fi) {
    // TODO check O_ACCMODE, also check if dir ??
    auto node = fcc()->get(path);
    if(fi->flags & O_CREAT && node == nullptr) {
        node = fcc()->add_op_key(path);
        if(node == nullptr) {
            return -ENOTSUP;
        }
    }
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG) {
        return -ENOTSUP;
    }
    if(fi->flags & O_TRUNC) {
        node->data.bytes.clear();
    }

    fi->fh = reinterpret_cast<uint64_t>(node);
    return 0;
}

static int cascade_fs_create(const char* path, mode_t mode,
                             struct fuse_file_info* fi) {
    // TODO check mode ? currently ignores
    return cascade_fs_open(path, fi);
}

static int cascade_fs_read(const char* path, char* buf, size_t size, off_t offset,
                           struct fuse_file_info* fi) {
    // auto node = reinterpret_cast<FSTree::Node*>(fi->fh);
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG) {  // TODO diff error
        return -EACCES;
    }
    auto& bytes = node->data.bytes;

    size_t len = bytes.size();

    if((size_t)offset < len) {
        if(offset + size > len) {
            size = len - offset;
        }
        auto p = reinterpret_cast<char*>(bytes.data());
        memcpy(buf, p + offset, size);
    } else {
        size = 0;
    }

    return size;
}

static int cascade_fs_write(const char* path, const char* buf, size_t size,
                            off_t offset, struct fuse_file_info* fi) {
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG || !node->data.writeable) {  // TODO diff error
        return -ENOTSUP;
    }
    auto& bytes = node->data.bytes;

    bytes.resize(std::max(bytes.size(), offset + size));  // TODO -ENOMEM
    // TODO potentially undefined?
    memcpy(bytes.data() + offset, buf, size);

    return size;
}

/*
static int cascade_fs_flush(const char* path, struct fuse_file_info* fi) {
    // TODO impl? (release is only called once at end while flush is called many times)
    // There is no guarantee that flush will ever be called at all
    return -ENOSYS;
}
*/

static int cascade_fs_release(const char* path, struct fuse_file_info* fi) {
    auto node = fcc()->get(path);
    if((fi->flags & O_ACCMODE) == O_RDONLY) {
        return 0;
    }
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG || !node->data.writeable) {
        return -ENOTSUP;
    }
    return fcc()->put_to_capi(node);
}

static int cascade_fs_mkdir(const char* path, mode_t mode) {
    if(fcc()->get(path)) {
        return -EEXIST;
    }
    auto op_root = fcc()->nearest_object_pool_root(path);
    if(op_root == nullptr) {
        return fcc()->add_snapshot(path) ? 0 : -EACCES;
    }
    auto res = fcc()->add_op_key_dir(path);
    if(res) {
        fcc()->local_latest_dirs.insert(path);
        // TODO err if nullptr
    }

    return 0;
}

static int cascade_fs_unlink(const char* path) {
    // return -ENOTSUP;
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG) {
        return -EISDIR;
    }
    // TODO check open

    // remove
    node->data.bytes.clear();

    // auto result = capi.remove(key);
    // node->parent->children.erase(node->label);
    // delete node;

    return 0;
}

static int cascade_fs_rmdir(const char* path) {
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    if(!(node->data.flag & DIR_FLAG)) {
        return -ENOTDIR;
    }
    if((node->data.flag & KEY_DIR) == 0) {
        return -EACCES;
    }
    if(!node->children.empty()) {
        return -ENOTEMPTY;
    }

    // remove
    fcc()->local_latest_dirs.erase(path);
    node->parent->children.erase(node->label);
    delete node;

    return 0;
}

static int cascade_fs_truncate(const char* path, off_t size,
                               struct fuse_file_info* fi) {
    auto node = fcc()->get(path);
    if(node == nullptr) {
        return -ENOENT;
    }
    if(node->data.flag & DIR_FLAG || !node->data.writeable) {
        return -EINVAL;
    }
    node->data.bytes.resize(size, 0);
    return fcc()->put_to_capi(node);
}

int set_buffer(char* dest, size_t size, const char* src, size_t len = 0) {
    if(len == 0) {
        len = strlen(src);
    }
    if(size == 0) {
        return len;
    } else if(size < len) {
        return -ERANGE;
    }
    memcpy(dest, src, len);
    return len;
}

static int cascade_fs_chmod(const char* path, mode_t mode, struct fuse_file_info* fi) {
    // TODO implement or -ENOTSUP
    return 0;
}

static int cascade_fs_chown(const char* path, uid_t uid, gid_t gid, struct fuse_file_info* fi) {
    return -ENOTSUP;
}

static int cascade_fs_utimens(const char*, const struct timespec tv[2], struct fuse_file_info* fi) {
    // TODO implement or -ENOTSUP
    return 0;
}

#ifdef HAVE_SETXATTR

/*
static int cascade_fs_setxattr(const char* path, const char* name, const char* value,
                               size_t size, int flags) {
    if(flags & XATTR_CREATE) {
        return -EPERM;
    }
    dbg_default_info("{}, {}, {}", path, name, value);
    // TODO save into node data
    if(strcmp(path, fcc()->ROOT) == 0) {
        if(!fcc()->latest && strcmp(name, "user.cascade.version") == 0) {
            fcc()->ver = strtoll(value, nullptr, 0);
            // TODO
            fcc()->update_object_pools();
            return 0;
        } else if(strcmp(name, "user.cascade.latest") == 0) {
            fcc()->latest = strcmp(value, "1") == 0;
            fcc()->update_object_pools();
            return 0;
        }
    }
    return -EPERM;
}
*/

static int cascade_fs_getxattr(const char* path, const char* name, char* value,
                               size_t size) {
    // TODO need to first apt get-install attr
    if(strcmp(path, fcc()->ROOT.c_str()) == 0) {
        if(strcmp(name, "user.cascade.largest_known_version") == 0) {
            std::string v = std::to_string(fcc()->max_ver);
            return set_buffer(value, size, v.c_str());
        }
    }
    return -ENODATA;
}

static int cascade_fs_listxattr(const char* path, char* list, size_t size) {
    // TODO lesson learned :(. returned 0 instead of length. check over all return types
    if(strcmp(path, fcc()->ROOT.c_str()) == 0) {
        // ^ 1hr+ bug
        const char names[] = "user.cascade.largest_known_version"
                             "\0";
        //  "user.cascade.version"
        //  "\0"
        //  "user.cascade.latest"
        //  "\0";
        // very weird behavior with \0 termination. strings and determining sizes did not work well
        // ^ 2hr+ bug
        return set_buffer(list, size, names, sizeof(names) / sizeof(names[0]) - 1);
    }
    const char empty[] = "";
    return set_buffer(list, size, empty, 0);
}

#endif

// TODO create_object_pool, remove_object_pool, get_object_pool, op_remove
// op_get size. maybe no need to cache data?
// TODO version/#/... (mount)
// maybe use attr

static const struct fuse_operations cascade_fs_oper = {
        .getattr = cascade_fs_getattr,
        // truncate, utimens, chown in place of setattr
        .mkdir = cascade_fs_mkdir,
        .unlink = cascade_fs_unlink,
        .rmdir = cascade_fs_rmdir,
        .chmod = cascade_fs_chmod,
        .chown = cascade_fs_chown,
        .truncate = cascade_fs_truncate,
        .open = cascade_fs_open,
        .read = cascade_fs_read,
        .write = cascade_fs_write,
        // .flush = cascade_fs_flush,
        .release = cascade_fs_release,
#ifdef HAVE_SETXATTR
        // .setxattr = cascade_fs_setxattr,
        .getxattr = cascade_fs_getxattr,
        .listxattr = cascade_fs_listxattr,
#endif
        .readdir = cascade_fs_readdir,
        .init = cascade_fs_init,
        .destroy = cascade_fs_destroy,
        .create = cascade_fs_create,
        .utimens = cascade_fs_utimens};

bool prepare_derecho_conf_file(const char* config_dir) {
    // TODO for some reason needs to already be in correct directory ???
    // maybe derecho_conf_file envvar not correct?
    // std::filesystem::path p(config_dir);
    // const std::string conf_file = (p / "derecho.cfg").u8string();

    // std::filesystem::current_path(config_dir);
    const std::string conf_file = (std::filesystem::current_path() / "derecho.cfg").u8string();

    struct stat buffer;
    if(stat(conf_file.c_str(), &buffer) != 0) {
        return false;
    }

    const char* const derecho_conf_file = "DERECHO_CONF_FILE";
    setenv(derecho_conf_file, conf_file.c_str(), false);
    dbg_default_info("Using derecho config file: {}.", getenv(derecho_conf_file));
    return true;
}

int main(int argc, char* argv[]) {
    options.client_dir = strdup(".");

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse* fuse = nullptr;
    struct fuse_cmdline_opts opts;
    struct fuse_loop_config config;
    int res = 0;

    if(fuse_opt_parse(&args, &options, option_spec, nullptr) == -1) {
        return 1;
    }

    if(fuse_parse_cmdline(&args, &opts) != 0) {
        return 1;
    }

    try {
        if(opts.show_version) {
            printf("FUSE library version %s\n", fuse_pkgversion());
            fuse_lowlevel_version();
            res = 0;
            throw 1;
        } else if(opts.show_help) {
            show_help(argv[0]);
            fuse_cmdline_help();
            // fuse_lib_help(&args);
            res = 0;
            throw 1;
        } else if(!opts.mountpoint) {
            fprintf(stderr, "error: no mountpoint specified\n");
            res = 1;
            throw 1;
        } else if(!opts.singlethread) {
            fprintf(stderr, "error: multi-threaded client not supported\n");
            res = 1;
            throw 1;
        } else if(!prepare_derecho_conf_file(options.client_dir)) {
            fprintf(stderr, "error: invalid client directory\n"
                            "(dir needs derecho.cfg if DERECHO_CONF_FILE envvar is not set)\n");
            res = 1;
            throw 1;
        }

        try {
            fuse = fuse_new(&args, &cascade_fs_oper, sizeof(cascade_fs_oper), nullptr);
            if(fuse == nullptr) {
                res = 1;
                throw 1;
            }

            if(fuse_mount(fuse, opts.mountpoint) != 0) {
                res = 1;
                throw 2;
            }

            if(fuse_daemonize(opts.foreground) != 0) {
                res = 1;
                throw 3;
            }

            struct fuse_session* se = fuse_get_session(fuse);
            if(fuse_set_signal_handlers(se) != 0) {
                res = 1;
                throw 3;
            }
            if(opts.singlethread) {
                res = fuse_loop(fuse);
            } else {
                config.clone_fd = opts.clone_fd;
                config.max_idle_threads = opts.max_idle_threads;
                res = fuse_loop_mt(fuse, &config);
            }
            if(res) {
                res = 1;
            }

            fuse_remove_signal_handlers(se);
        } catch(derecho::derecho_exception& ex) {
            std::cerr << "Exception:" << ex.what() << std::endl;
            // TODO errors are not caught :( like cannot bind to socket
            res = 1;
        }

        throw 3;
    } catch(int& ex) {
        switch(ex) {
            case 3:
                fuse_unmount(fuse);
            case 2:
                fuse_destroy(fuse);
            case 1:
                free(opts.mountpoint);
                fuse_opt_free_args(&args);
        }
    }

    return res;
}
