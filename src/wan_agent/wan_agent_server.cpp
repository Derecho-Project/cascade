#include <algorithm>
#include <arpa/inet.h>
#include <condition_variable>
#include <derecho/utils/logger.hpp>
#include <fstream>
#include <list>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <wan_agent/wan_agent.hpp>
using std::cout;
using std::endl;

int main(int argc, char** argv) {
    // TODO: should use code in wan_agent/wan_agent.hpp, not duplicated code.

    if(argc < 2) {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "wan_agent configuration file" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    std::string json_config = argv[1];
    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    wan_agent::RemoteMessageCallback rmc = [](const uint32_t from, const char* msg, const size_t size) {
        std::cout << "message received from site:" << from
                  << ", message size:" << size << " bytes"
                  << std::endl;
    };

    wan_agent::WanAgentServer w_server(conf, rmc);
    return 0;
}