#include <cascade/service_types.hpp>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fuse_client_context.hpp"
#include "fuse_client_signals.hpp"

#define FUSE_CLIENT_DEV_ID (0xCA7CADE)

/**
 * fuse_client mounts the cascade service as a file system. This allows users to access cascade data with normal POSIX
 * filesystem API.
 *
 * TODO document data path for object pool
 *
 * The data in cascade is organized this way:
 * <mount-point>/<site-name>/<subgroup-type>/<subgroup-index>/<shard-index>/<key>
 * "mount-point" is where the cascade data is mounted.
 * "site-name" is for the name of the data center. "localsite" is the alias for local data center. Currently, only
 *      "localsite" is enabled. Names for data centers will be enabled by wanagent.
 * "subgroup-type" is the name for the subgroup types.
 * "subgroup-index" is the index of a subgroup of corresponding subgroup type.
 * "shard-index" is the index of the shard in corresponding shard type.
 * "key" is the key value.
 */

using namespace derecho::cascade;
using FuseClientContextType = FuseClientContext<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>;

// #define FCC(p) static_cast<FuseClientContextType*>(p)
// #define fcc(req) FCC(fuse_req_userdata(req))

static FuseClientContextType* fcc(fuse_req_t req) {
    return static_cast<FuseClientContextType*>(fuse_req_userdata(req));
}

static void fs_init(void* userdata, struct fuse_conn_info* conn) {
    dbg_default_trace("entering {}.", __func__);
    auto fcc = static_cast<FuseClientContextType*>(userdata);
    // TODO look through conn->capable options

    if(derecho::hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT)) {
        fcc->initialize(json::parse(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT)));
    } else if(derecho::hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT_FILE)) {
        nlohmann::json layout_array;
        std::ifstream json_file(derecho::getAbsoluteFilePath(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE)));
        if(!json_file) {
            dbg_default_error("Cannot load json configuration from file: {}", derecho::getAbsoluteFilePath(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE)));
            throw derecho::derecho_exception("Cannot load json configuration from file.");
        }
        json_file >> layout_array;
        fcc->initialize(layout_array);
    }
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_destroy(void* userdata) {
    dbg_default_trace("entering {}.", __func__);
    dbg_default_trace("leaving {}.", __func__);
}

static int do_lookup(fuse_req_t req, fuse_ino_t parent, const char* name,
                     struct fuse_entry_param* e) {
    // TODO: make this more efficient by implement a dedicated call in FCC.
    auto name_to_ino = fcc(req)->get_dir_entries(parent);
    if(name_to_ino.find(name) == name_to_ino.end()) {
        return ENOENT;
    }
    // TODO: change timeout settings.
    e->ino = name_to_ino.at(name);
    e->attr_timeout = 10000.0;
    e->entry_timeout = 10000.0;
    e->attr.st_ino = e->ino;
    fcc(req)->fill_stbuf_by_ino(e->attr);

    return 0;
}

static void fs_lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    dbg_default_trace("entering {}.", __func__);
    struct fuse_entry_param e;

    int err = do_lookup(req, parent, name, &e);
    if(err) {
        fuse_reply_err(req, err);
    } else {
        fuse_reply_entry(req, &e);
    }

    // auto name_to_ino = fcc(req)->get_dir_entries(parent);
    // if(name_to_ino.find(name) == name_to_ino.end()) {
    //     fuse_reply_err(req, ENOENT);
    // } else {
    //     // TODO: change timeout settings.
    //     e.ino = name_to_ino.at(name);
    //     e.attr_timeout = 10000.0;
    //     e.entry_timeout = 10000.0;
    //     e.attr.st_ino = e.ino;
    //     fcc(req)->fill_stbuf_by_ino(e.attr);
    //     fuse_reply_entry(req, &e);
    // }

    dbg_default_trace("leaving {}.", __func__);
}

static void fs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    struct stat stbuf;  // TODO memory leak?
    (void)fi;

    std::memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fcc(req)->fill_stbuf_by_ino(stbuf);

    fuse_reply_attr(req, &stbuf, 10000.0);
    dbg_default_trace("leaving {}.", __func__);
}

// borrowed from libfuse \a hello_ll.c
struct dirbuf {
    char* p;
    size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf* b, const char* name, fuse_ino_t ino) {
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_add_direntry(req, nullptr, 0, name, nullptr, 0);
    b->p = (char*)realloc(b->p, b->size);
    std::memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fcc(req)->fill_stbuf_by_ino(stbuf);  // additional effect
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf, b->size);
}

static int reply_buf_limited(fuse_req_t req, const char* buf, size_t bufsize,
                             off_t off, size_t maxsize) {
    if(static_cast<size_t>(off) < bufsize)
        return fuse_reply_buf(req, buf + off, std::min(bufsize - off, maxsize));
    else
        return fuse_reply_buf(req, nullptr, 0);
}

static void fs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    struct dirbuf b;
    std::memset(&b, 0, sizeof(b));
    dirbuf_add(req, &b, ".", 1);
    dirbuf_add(req, &b, "..", 1);
    for(auto kv : fcc(req)->get_dir_entries(ino)) {
        dirbuf_add(req, &b, kv.first.c_str(), kv.second);
    }
    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    int err;
    if((fi->flags & O_ACCMODE) != O_RDONLY && !fcc(req)->is_writable(ino)) {
        // allow write for key nodes
        fuse_reply_err(req, EACCES);
    } else if((err = fcc(req)->open_file(ino, fi)) != 0) {
        fuse_reply_err(req, err);
    } else {
        dbg_default_debug("fi({:x})->fh={:x}", reinterpret_cast<uint64_t>(fi), fi->fh);
        fuse_reply_open(req, fi);
    }
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);

    // TODO `file_bytes.bytes` might be freed in reply buf once fs_read ends. may need to go back to storing in `fi`
    FileBytes file_bytes;
    int res = fcc(req)->read_file(ino, size, off, fi, &file_bytes);
    if(res == 0) {
        reply_buf_limited(req, reinterpret_cast<char*>(file_bytes.bytes.data()), file_bytes.bytes.size(), off, size);
    } else {
        // TODO handle error
        fuse_reply_err(req, 1);
    }

    dbg_default_trace("leaving {}.", __func__);
}

static void fs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    fcc(req)->close_file(ino, fi);
    fuse_reply_err(req, 0);
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t off,
                     struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);

    // TODO write_buf instead?... use fi??
    ssize_t res = fcc(req)->write_file(ino, buf, size, off, fi);
    if(res < 0) {
        fuse_reply_err(req, -res);
    } else {
        fuse_reply_write(req, (size_t)res);
    }

    dbg_default_trace("leaving {}.", __func__);
}

static void fs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                       int valid, struct fuse_file_info* fi) {
    dbg_default_trace("setattr(ino={%u}, valid: {%o}).", ino, valid);

    //     char procname[64];
    //     struct lo_inode* inode = lo_inode(req, ino);
    //     int ifd = inode->fd;
    int res;

    try {
        if(!fi) {
            errno = EACCES;
            throw 1;
        }
        if(valid & FUSE_SET_ATTR_SIZE) {
            dbg_default_trace("attr_size(length={})", (unsigned long)attr->st_size);

            if(!fcc(req)->is_writable(ino)) {
                errno = EACCES;
                throw 1;
            }
            res = fcc(req)->truncate(ino, attr->st_size, fi);

            if(res == -1) {
                errno = EACCES;
                throw 1;
            }
        }
        // if(valid & FUSE_SET_ATTR_MODE) {}
        // if(valid & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {}
        // if(valid & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {}
        return fs_getattr(req, ino, fi);
    } catch(int& ex) {
        int saverr = errno;
        fuse_reply_err(req, saverr);
    }
}

static void fs_create(fuse_req_t req, fuse_ino_t parent, const char* name,
                      mode_t mode, struct fuse_file_info* fi) {
    struct fuse_entry_param e;
    int err;

    dbg_default_info("fs create: {}", name);
    // TODO fi fields like direct_io and keep_cache

    err = fcc(req)->open_at(parent, name, mode, fi);
    if(err) {
        return (void)fuse_reply_err(req, err);
    }

    err = do_lookup(req, parent, name, &e);
    if(err)
        fuse_reply_err(req, err);
    else
        fuse_reply_create(req, &e, fi);
}

static void fs_mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
                     mode_t mode, dev_t rdev) {
    dbg_default_info("fs mknod: {}", name);
    fuse_reply_err(req, 1);
}

static void fs_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name,
                     mode_t mode) {
    dbg_default_info("fs mkdir: {}", name);
    fuse_reply_err(req, 1);
}

static const struct fuse_lowlevel_ops fs_ops = {
        .init = fs_init,
        .destroy = fs_destroy,
        .lookup = fs_lookup,
        .forget = nullptr,
        .getattr = fs_getattr,
        .setattr = fs_setattr,
        .readlink = nullptr,
        .mknod = fs_mknod,
        .mkdir = fs_mkdir,
        .unlink = nullptr,
        .rmdir = nullptr,
        .symlink = nullptr,
        .rename = nullptr,
        .link = nullptr,
        .open = fs_open,
        .read = fs_read,
        .write = fs_write,
        .release = fs_release,
        .fsync = nullptr,
        .opendir = nullptr,
        .readdir = fs_readdir,
        .create = fs_create,
};

/**
 * According to our experiment as well as recorded in
 * [this](https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201109/homework/fuse/fuse_doc.html) document as following:
 * " When it starts, Fuse changes its working directory to "/". That will probably break any code that uses relative
 *   pathnames. To make matters worse, the chdir is suppressed when you run with the -f switch, so your code might
 *   appear to work fine under the debugger. To avoid the problem, either (a) use absolute pathnames, or (b) record
 *   your current working directory by calling get_current_dir_name before you invoke fuse_main, and then convert
 *   relative pathnames into corresponding absolute ones. Obviously, (b) is the preferred approach. ",
 * we need to prepare the configuration file path to make sure the \a ServerClient is able to access the configuration
 * file.
 */
void prepare_derecho_conf_file() {
    std::filesystem::path p = std::filesystem::current_path() / "derecho.cfg";
    const std::string conf_file = p.u8string();

    const char* const derecho_conf_file = "DERECHO_CONF_FILE";
    setenv(derecho_conf_file, conf_file.c_str(), false);
    dbg_default_debug("Using derecho config file: {}.", getenv(derecho_conf_file));

    //     char cwd[4096];
    // #pragma GCC diagnostic ignored "-Wunused-result"
    //     getcwd(cwd, 4096);
    // #pragma GCC diagnostic pop
    //     sprintf(cwd + strlen(cwd), "/derecho.cfg");
    //     setenv("DERECHO_CONF_FILE", cwd, false);
    // dbg_default_debug("Using derecho config file:{}.", getenv("DERECHO_CONF_FILE"));
}

int main(int argc, char** argv) {
    prepare_derecho_conf_file();

    // TODO extra args like cache level / specifying timeout
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_session* se = nullptr;
    struct fuse_cmdline_opts opts;
    int ret = -1;

    if(fuse_parse_cmdline(&args, &opts) != 0) {
        return ret;
    }

    try {
        if(opts.show_help) {
            std::cout << "usage: " << argv[0] << " [options] <mountpoint>\n"
                      << std::endl;
            fuse_cmdline_help();
            fuse_lowlevel_help();
            ret = 0;
            throw 1;
        } else if(opts.show_version) {
            std::cout << "FUSE library version " << fuse_pkgversion() << std::endl;
            fuse_lowlevel_version();
            ret = 0;
            throw 1;
        }

        if(opts.mountpoint == nullptr) {
            std::cout << "usage: " << argv[0] << " [options] <mountpoint>\n"
                      << "       " << argv[0] << " --help" << std::endl;
            ret = 1;
            throw 1;
        }

        // start session
        // TODO fcc hangs forever when no server nodes running
        // fcc also causes signal handlers to not register in fuse_set_signal_handlers
        // problem: ServiceClientAPI::get_service_client() registers SIGINT and SIGTERM handlers
        // (proof in logging??: [trace] Polling thread ending.)
        // fuse_set_signal_handlers only sets when replacing SIG_DFL

        FuseClientContextType fcc;
        if(fuse_client_signals::store_old_signal_handlers() == -1) {
            dbg_default_error("could not store old signal handlers");
            ret = 1;
            throw 1;
        }

        dbg_default_info("start session");
        se = fuse_session_new(&args, &fs_ops, sizeof(fs_ops), &fcc);
        if(se == nullptr) {
            throw 1;
        }
        if(fuse_set_signal_handlers(se) != 0) {
            throw 2;
        }
        if(fuse_session_mount(se, opts.mountpoint) != 0) {
            throw 3;
        }

        fuse_daemonize(opts.foreground);

        /* Block until ctrl+c or fusermount -u */
        dbg_default_info("starting fuse client.");
        if(opts.singlethread) {
            ret = fuse_session_loop(se);
        } else {
            std::cout << "Multi-threaded client not supported yet" << std::endl;
            ret = 1;
            // ret = fuse_session_loop_mt(se, opts.clone_fd);
        }

        dbg_default_info("ending fuse.");
        fuse_session_unmount(se);
        throw 3;
    } catch(int& ex) {
        switch(ex) {
            case 3:
                fuse_remove_signal_handlers(se);
            case 2:
                fuse_session_destroy(se);
            case 1:
                if(fuse_client_signals::restore_old_signal_handlers() == -1) {
                    dbg_default_error("could not restore old signal handlers");
                    ret = 1;
                }
                free(opts.mountpoint);
                fuse_opt_free_args(&args);
        }
    }

    return ret;
}
