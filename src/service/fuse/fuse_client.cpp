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

#define FCC(p) static_cast<FuseClientContextType*>(p)
#define FCC_REQ(req) FCC(fuse_req_userdata(req))

static void fs_init(void* userdata, struct fuse_conn_info* conn) {
    dbg_default_trace("entering {}.", __func__);
    if(derecho::hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT)) {
        FCC(userdata)->initialize(json::parse(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT)));
    } else if(derecho::hasCustomizedConfKey(CONF_LAYOUT_JSON_LAYOUT_FILE)) {
        nlohmann::json layout_array;
        std::ifstream json_file(derecho::getAbsoluteFilePath(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE)));
        if(!json_file) {
            dbg_default_error("Cannot load json configuration from file: {}", derecho::getAbsoluteFilePath(derecho::getConfString(CONF_LAYOUT_JSON_LAYOUT_FILE)));
            throw derecho::derecho_exception("Cannot load json configuration from file.");
        }
        json_file >> layout_array;
        FCC(userdata)->initialize(layout_array);
    }
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_destroy(void* userdata) {
    dbg_default_trace("entering {}.", __func__);
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    dbg_default_trace("entering {}.", __func__);
    struct fuse_entry_param e;
    // TODO: make this more efficient by implement a dedicated call in FCC.
    auto name_to_ino = FCC_REQ(req)->get_dir_entries(parent);
    if(name_to_ino.find(name) == name_to_ino.end()) {
        fuse_reply_err(req, ENOENT);
    } else {
        // TODO: change timeout settings.
        e.ino = name_to_ino.at(name);
        e.attr_timeout = 10000.0;
        e.entry_timeout = 10000.0;
        e.attr.st_ino = e.ino;
        FCC_REQ(req)->fill_stbuf_by_ino(e.attr);
        fuse_reply_entry(req, &e);
    }

    dbg_default_trace("leaving {}.", __func__);
}

static void fs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    struct stat stbuf;
    (void)fi;

    std::memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    FCC_REQ(req)->fill_stbuf_by_ino(stbuf);

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
    FCC_REQ(req)->fill_stbuf_by_ino(stbuf);  // additional effect
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf, b->size);
}

static int reply_buf_limited(fuse_req_t req, const char* buf, size_t bufsize,
                             off_t off, size_t maxsize) {
    if(static_cast<size_t>(off) < bufsize)
        return fuse_reply_buf(req, buf + off, std::min(bufsize - off, maxsize));
    else
        return fuse_reply_buf(req, NULL, 0);
}

static void fs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    struct dirbuf b;
    std::memset(&b, 0, sizeof(b));
    dirbuf_add(req, &b, ".", 1);
    dirbuf_add(req, &b, "..", 1);
    for(auto kv : FCC_REQ(req)->get_dir_entries(ino)) {
        dirbuf_add(req, &b, kv.first.c_str(), kv.second);
    }
    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    int err;
    if((fi->flags & O_ACCMODE) != O_RDONLY && !FCC_REQ(req)->is_writable(ino)) {
        // allow write for key nodes
        fuse_reply_err(req, EACCES);
    } else if((err = FCC_REQ(req)->open_file(ino, fi)) != 0) {
        fuse_reply_err(req, err);
    } else {
        dbg_default_debug("fi({:x})->fh={:x}", reinterpret_cast<uint64_t>(fi), fi->fh);
        fuse_reply_open(req, fi);
    }
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);

    FileBytes* pfb = reinterpret_cast<FileBytes*>(fi->fh);
    reply_buf_limited(req, reinterpret_cast<char*>(pfb->bytes), pfb->size, off, size);

    dbg_default_trace("leaving {}.", __func__);
}

static void fs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);
    FCC_REQ(req)->close_file(ino, fi);
    fuse_reply_err(req, 0);
    dbg_default_trace("leaving {}.", __func__);
}

static void fs_write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t off,
                     struct fuse_file_info* fi) {
    dbg_default_trace("entering {}.", __func__);

    // TODO write_buf instead?... use fi??
    ssize_t res = FCC_REQ(req)->write_file(ino, buf, size, off, fi);
    if(res < 0) {
        fuse_reply_err(req, -res);
    } else {
        fuse_reply_write(req, (size_t)res);
    }

    dbg_default_trace("leaving {}.", __func__);
}

static const struct fuse_lowlevel_ops fs_ops = {
        .init = fs_init,
        .destroy = fs_destroy,
        .lookup = fs_lookup,
        .forget = NULL,
        .getattr = fs_getattr,
        .setattr = NULL,
        .readlink = NULL,
        .mknod = NULL,
        .mkdir = NULL,
        .unlink = NULL,
        .rmdir = NULL,
        .symlink = NULL,
        .rename = NULL,
        .link = NULL,
        .open = fs_open,
        .read = fs_read,
        .write = fs_write,
        .release = fs_release,
        .fsync = NULL,
        .opendir = NULL,
        .readdir = fs_readdir,
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
