#include <cascade/service.hpp>
#include <sys/prctl.h>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#define PROC_NAME "cascade_service"

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING"

int main(int argc, char** argv) {
    // set proc name
    if( prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0 ) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }
    // load configuration
    std::cout << derecho::getConfString(CONF_VCS_UINT64KEY_LAYOUT) << std::endl;
    json vcs_uint64key_layout = json::parse(derecho::getConfString(CONF_VCS_UINT64KEY_LAYOUT));
    std::cout << vcs_uint64key_layout.dump() << std::endl;
    return 0;
}
