#include "fuse_client_signals.hpp"

#include <map>
#include <signal.h>
#include <string.h>

using handler_t = void (*)(int);
static std::map<int, handler_t> old_signal_handlers;
static const int signals[] = {SIGHUP, SIGINT, SIGTERM, SIGPIPE};

static int set_one_signal_handler(int sig, int remove) {
    if(remove) {
        struct sigaction old_sa;
        if(sigaction(sig, NULL, &old_sa) == -1) {
            return -1;
        }
        old_signal_handlers[sig] = old_sa.sa_handler;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    bool contains = old_signal_handlers.find(sig) == old_signal_handlers.end();
    sa.sa_handler = remove || !contains ? SIG_DFL : old_signal_handlers[sig];
    sigemptyset(&(sa.sa_mask));
    sa.sa_flags = 0;
    if(sigaction(sig, &sa, NULL) == -1) {
        return -1;
    }

    return 0;
}

int fuse_client_signals::store_old_signal_handlers() {
    for(int sig : signals) {
        if(set_one_signal_handler(sig, true) == -1) {
            return -1;
        }
    }
    return 0;
}

int fuse_client_signals::restore_old_signal_handlers() {
    for(int sig : signals) {
        if(set_one_signal_handler(sig, false) == -1) {
            return -1;
        }
    }
    return 0;
}
