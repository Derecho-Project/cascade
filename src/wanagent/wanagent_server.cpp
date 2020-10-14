#include "wanagent_utils.cpp"
#include <algorithm>
#include "wanagent/wanagent_type_definitions.hpp"
#include <arpa/inet.h>
#include <condition_variable>
#include <list>
#include <derecho/utils/logger.hpp>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <fstream>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>

struct RequestHeader {
    uint64_t seq;
    uint32_t site_id;
    size_t payload_size;
};

struct Response {
    uint64_t seq;
    uint32_t site_id;
};

using RemoteMessageCallback = std::function<void(const site_id_t, const char*, const size_t)>;
using NotifierFunc = std::function<void()>;
using std::cout;
using std::endl;

// configuration entries 
#define WAN_AGENT_CONF_VERSION          "version"
#define WAN_AGENT_CONF_TRANSPORT        "transport"
#define WAN_AGENT_CONF_PRIVATE_IP       "private_id"
#define WAN_AGENT_CONF_PRIVATE_PORT     "private_port"
#define WAN_AGENT_CONF_LOCAL_SITE_ID    "local_site_id"
#define WAN_AGENT_CONF_SITES            "sites"
#define WAN_AGENT_CONF_SITES_ID         "id"
#define WAN_AGENT_CONF_SITES_IP         "ip"
#define WAN_AGENT_CONF_SITES_PORT       "port"
#define WAN_AGENT_MAX_PAYLOAD_SIZE      "max_payload_size"
#define WAN_AGENT_WINDOW_SIZE           "window_size"

class RemoteMessageService final {
private:
    const site_id_t local_site_id;
    const std::map<site_id_t, std::pair<ip_addr_t, uint16_t>>& sites_ip_addrs_and_ports;
    const size_t max_payload_size;
    const RemoteMessageCallback rmc;

    const NotifierFunc ready_notifier;
    std::atomic<bool> server_ready;
    std::list<std::thread> worker_threads;

    int server_socket;
    /**
     * configuration
     */
    const nlohmann::json config;


public:
     RemoteMessageService(const site_id_t local_site_id,
                         const std::map<site_id_t, std::pair<ip_addr_t, uint16_t>>& sites_ip_addrs_and_ports,
                         const size_t max_payload_size,
                         const RemoteMessageCallback& rmc,
                         const NotifierFunc& ready_notifier_lambda) : local_site_id(local_site_id),
                                                                      sites_ip_addrs_and_ports(sites_ip_addrs_and_ports),
                                                                      max_payload_size(max_payload_size),
                                                                      rmc(rmc),
                                                                      ready_notifier(ready_notifier_lambda),
                                                                      server_ready(false){
        std::cout << "1: " << local_site_id << std::endl;  
        std::cout << "map size: " << sites_ip_addrs_and_ports.size() << std::endl;                                                                    
        auto local_port = sites_ip_addrs_and_ports.at(local_site_id).second;
        std::cout << "2" << std::endl;      
        sockaddr_in serv_addr;
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if(fd < 0) throw std::runtime_error("RemoteMessageService failed to create socket.");

        int reuse_addr = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse_addr,
                   sizeof(reuse_addr)) < 0) {
                       fprintf(stderr, "ERROR on setsockopt: %s\n", strerror(errno));
                   }

        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(local_port);
        if(bind(fd, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr,"ERROR on binding to socket: %s\n", strerror(errno));
            throw std::runtime_error("RemoteMessageService failed to bind socket.");
        }
        listen(fd, 5);
        server_socket = fd;
        std::cout << "RemoteMessageService listening on " << local_port << std::endl;
        // dbg_default_info("RemoteMessageService listening on {} ...", local_port);
    };

    void establish_connections() {

        // TODO: maybe support dynamic join later, i.e. having a infinite loop always listening for join requests?
        while(worker_threads.size() < sites_ip_addrs_and_ports.size() - 1) {  
            struct sockaddr_storage client_addr_info;
            socklen_t len = sizeof client_addr_info;

            int sock = ::accept(server_socket, (struct sockaddr*)&client_addr_info, &len);
            worker_threads.emplace_back(std::thread(&RemoteMessageService::worker, this, sock));
        }
        server_ready.store(true);
        std::cout << "established" << std::endl;
        ready_notifier();
    }

    void worker(int sock) {
        RequestHeader header;
        bool success;
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(max_payload_size);
        std::cout << "worker start" << std::endl;
        while(1) {
            if(sock < 0) throw std::runtime_error("sock closed!");

            success = sock_read(sock, header);
            if(!success) throw std::runtime_error("Failed to read request header");

            success = sock_read(sock, buffer.get(), header.payload_size);
            if(!success) throw std::runtime_error("Failed to read message");

            // dbg_default_info("received msg {} from site {}", header.seq, header.site_id);

            rmc(header.site_id, buffer.get(), header.payload_size);
            success = sock_write(sock, Response{header.seq, local_site_id});
            if(!success) throw std::runtime_error("Failed to send ACK message");
        }
    }

    bool is_server_ready() {
        return server_ready.load();
    }
};

void check_json(const nlohmann::json& config){
    static const std::vector<std::string> must_have{
        WAN_AGENT_CONF_VERSION,
        WAN_AGENT_CONF_TRANSPORT,
        WAN_AGENT_CONF_LOCAL_SITE_ID,
        WAN_AGENT_CONF_SITES
    };
    for (auto& must_have_key : must_have) {
        if (config.find(must_have_key) == config.end()) {
            throw std::runtime_error(must_have_key + "is not found.");
        }
    }
    if (config[WAN_AGENT_CONF_SITES].size() == 0) {
        throw std::runtime_error("Sites does not have any configutaion.");
    }
}

std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> get_ip_ports(const nlohmann::json& config){
    std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> sites_ip_addrs_and_ports;
    for (auto& site : config[WAN_AGENT_CONF_SITES]) {
#define WAN_AGENT_CHECK_SITE_ENTRY(x) \
        if (site.find(x) == site.end()){ \
            throw std::runtime_error(std::string(x) + " missing in a site entry."); \
        }
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_ID);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_IP);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_PORT);
        sites_ip_addrs_and_ports.emplace(site[WAN_AGENT_CONF_SITES_ID], std::make_pair(site[WAN_AGENT_CONF_SITES_IP], site[WAN_AGENT_CONF_SITES_PORT]));
        // uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
        // message_counters->emplace(sid,0);
    }
}

int main(int argc, char** argv) {
    std::mutex ready_mutex;
    std::condition_variable ready_cv;

    if(argc < 2){
        cout << "Invalid command line arguments." << endl;
        cout << "USAGE:" << argv[0] << "wanagent configuration file" << endl;
        cout << "Thank you" << endl;
        return -1;
    }
    std::string json_config = argv[1];
    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;
    check_json(conf);
    cout << "check conf" << endl;
    RemoteMessageCallback rmc = [](const uint32_t from, const char* msg, const size_t size){
//                         std::cout << "message received from site:" << from 
  //                                 << ", message size:" << size << " bytes"
    //                               << std::endl;
                     };

    
    std::map<site_id_t, std::pair<ip_addr_t, uint16_t>> ip_ports;

    for (auto& site : conf[WAN_AGENT_CONF_SITES]) {
#define WAN_AGENT_CHECK_SITE_ENTRY(x) \
        if (site.find(x) == site.end()){ \
            throw std::runtime_error(std::string(x) + " missing in a site entry."); \
        }
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_ID);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_IP);
        WAN_AGENT_CHECK_SITE_ENTRY(WAN_AGENT_CONF_SITES_PORT);
        ip_ports.emplace(site[WAN_AGENT_CONF_SITES_ID], std::make_pair(site[WAN_AGENT_CONF_SITES_IP], site[WAN_AGENT_CONF_SITES_PORT]));
        // uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
        // message_counters->emplace(sid,0);
    }
    RemoteMessageService remote_message_service(conf[WAN_AGENT_CONF_LOCAL_SITE_ID],
                                                                           ip_ports,
                                                                           conf[WAN_AGENT_MAX_PAYLOAD_SIZE],
                                                                           rmc,
                                                                           [&ready_cv]() {ready_cv.notify_all();});
    std::thread rms_establish_thread(&RemoteMessageService::establish_connections, &remote_message_service);
    rms_establish_thread.detach();
    sleep(10);
    std::cout << "Press ENTER to kill." << std::endl;
    std::cin.get();
    
    return 0;
}