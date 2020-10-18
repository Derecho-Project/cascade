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
#include <wan_agent/wan_agent_type_definitions.hpp>
#include <wan_agent/wan_agent_utils.hpp>
#include <wan_agent/wan_agent.hpp>
using std::cout;
using std::endl;

void check_json(const nlohmann::json &config)
{
    static const std::vector<std::string> must_have{
        WAN_AGENT_CONF_VERSION,
        WAN_AGENT_CONF_TRANSPORT,
        WAN_AGENT_CONF_LOCAL_SITE_ID,
        WAN_AGENT_CONF_SITES};
    for (auto &must_have_key : must_have)
    {
        if (config.find(must_have_key) == config.end())
        {
            throw std::runtime_error(must_have_key + "is not found.");
        }
    }
    if (config[WAN_AGENT_CONF_SITES].size() == 0)
    {
        throw std::runtime_error("Sites does not have any configutaion.");
    }
}

int main(int argc, char **argv)
{
    // TODO: should use code in wan_agent/wan_agent.hpp, not duplicated code.

    if (argc < 2)
    {
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "wan_agent configuration file" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    std::string json_config = argv[1];
    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;
    check_json(conf);
    cout << "check conf" << endl;
    wan_agent::RemoteMessageCallback rmc = [](const uint32_t from, const char *msg, const size_t size) {
        std::cout << "message received from site:" << from
                  << ", message size:" << size << " bytes"
                  << std::endl;
    };

    std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> ip_ports;

    for (auto &site : conf[WAN_AGENT_CONF_SITES])
    {
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_ID);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_IP);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_PORT);
        ip_ports.emplace(site[WAN_AGENT_CONF_SITES_ID], std::make_pair(site[WAN_AGENT_CONF_SITES_IP], site[WAN_AGENT_CONF_SITES_PORT]));
        // uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
        // message_counters->emplace(sid,0);
    }
    wan_agent::RemoteMessageService remote_message_service(
        conf[WAN_AGENT_CONF_LOCAL_SITE_ID],
        ip_ports,
        conf[WAN_AGENT_MAX_PAYLOAD_SIZE],
        rmc,
        [&ready_cv]() { ready_cv.notify_all(); });
    std::thread rms_establish_thread(
        &wan_agent::RemoteMessageService::establish_connections, &remote_message_service);
    rms_establish_thread.detach();
    sleep(10);
    std::cout << "Press ENTER to kill." << std::endl;
    std::cin.get();

    return 0;
}