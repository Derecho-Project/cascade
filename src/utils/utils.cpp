#include <cascade/utils.hpp>
#include <memory>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include <chrono>
#include <math.h>
#include <utility>
#include <fstream>
#include <derecho/conf/conf.hpp>
#include <stack>
#include <cctype>
#include <map>

using namespace std::chrono_literals;

namespace derecho {
namespace cascade {

struct __attribute__((packed)) open_loop_ack_t {
    uint32_t type;
    uint32_t id;
    uint64_t ts_us;
};

class OpenLoopLatencyCollectorClientImpl: public OpenLoopLatencyCollectorClient {
private:
    int client_socket;
    struct sockaddr_in server_addr;
public:
    OpenLoopLatencyCollectorClientImpl(const std::string& hostname, uint16_t udp_port) {
        struct hostent* server;
        client_socket = socket(AF_INET, SOCK_DGRAM, 0);
        if (client_socket < 0) {
            std::cerr << "Failed to open socket:" << std::endl;
            return;
        }
        server = gethostbyname(hostname.c_str());
        if (server == nullptr) {
            std::cerr << "Failed to get host for:" << hostname << std::endl;
        }

        bzero((char *)&server_addr, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&server_addr.sin_addr, server->h_length);
        server_addr.sin_port = htons(udp_port);
    }

    virtual void ack(uint32_t type, uint32_t id, bool use_local_ts) override {
        struct open_loop_ack_t ack{type,id,0};

        if(use_local_ts) {
            ack.ts_us = get_time_us(true);
        }

        size_t ns = sendto(client_socket,static_cast<const void*>(&ack),
                           sizeof(open_loop_ack_t),0,reinterpret_cast<const sockaddr*>(&server_addr),sizeof(server_addr));
        if (ns < 0) {
            std::cerr << "WARNING: Failed to report (" << type << "," << id << ")" << std::endl;
        }
    }

    virtual ~OpenLoopLatencyCollectorClientImpl() {
        close(client_socket);
    }
};

std::unique_ptr<OpenLoopLatencyCollectorClient> OpenLoopLatencyCollectorClient::create_client(const std::string& hostname, uint16_t udp_port) {
    return std::make_unique<OpenLoopLatencyCollectorClientImpl>(hostname,udp_port);
}

OpenLoopLatencyCollector::OpenLoopLatencyCollector(uint32_t max_ids,const std::vector<uint32_t>& type_set,
                                                   const std::function<bool(const std::map<uint32_t,uint32_t>&)>& udp_acks_collected,
                                                   uint16_t udp_port):
    udp_acks_collected_predicate(udp_acks_collected),port(udp_port) {
    for (uint32_t type:type_set) {
        timestamps_in_us.emplace(std::piecewise_construct, std::make_tuple(type), std::make_tuple());
        timestamps_in_us.at(type).reserve(max_ids);
        counters.emplace(type,0);
    }
    stop = false;

    // start the server thread with lambda
    std::thread th([this](uint16_t port){
        struct sockaddr_in server_addr, client_addr;
        int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        const int BUFSIZE = 64;
        char buf[BUFSIZE];
        // STEP 1: start UDP channel
        if (sockfd < 0) {
            std::cerr << "ERROR opening socket" << std::endl;
            return;
        }
        int optval = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval, sizeof(int));
        // STEP 2: wait for UDP message
        bzero((char *)&server_addr, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        server_addr.sin_port = htons(port);
        if (bind(sockfd, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Fail to bind udp port:" << port << std::endl;
            return;
        }
        socklen_t client_len = sizeof(client_addr);
        while (!stop) {
            ssize_t nrecv = recvfrom(sockfd,buf,BUFSIZE,0,(struct sockaddr *) &client_addr,&client_len);
            if (nrecv < 0) {
                std::cerr << "Fail to recv udp package." << std::endl;
                continue;
            }
            open_loop_ack_t* ola = reinterpret_cast<open_loop_ack_t*>(buf);
            if(timestamps_in_us.find(ola->type) == timestamps_in_us.end()) {
                std::cerr << "unknown event type:" << ola->type << std::endl;
                continue;
            }
            if (ola->ts_us == 0) {
                timestamps_in_us.at(ola->type)[ola->id] = get_time_us(true);
            } else {
                timestamps_in_us.at(ola->type)[ola->id] = ola->ts_us;
            }
            counters.at(ola->type) ++;

            if (udp_acks_collected_predicate(counters)) {
                std::lock_guard<std::mutex> lck(stop_mutex);
                stop = true;
            }
        }

        stop_cv.notify_all();
    }, udp_port);

    server_thread = std::move(th);
}

bool OpenLoopLatencyCollector::wait(uint32_t nsec) {
    std::unique_lock<std::mutex> lck(stop_mutex);
    stop_cv.wait_for(lck,nsec*1s,[this](){return stop;});
    if (stop && server_thread.joinable()) {
        server_thread.join();
    }
    return stop;
}

void OpenLoopLatencyCollector::ack(uint32_t type,uint32_t id, bool) {
    if (timestamps_in_us.find(type) == timestamps_in_us.end()) {
        std::cerr << "unknown event type:" << type << std::endl;
        return;
    }
    timestamps_in_us.at(type)[id] = get_time_us(true);
    counters.at(type) ++;
}

std::tuple<double,double,uint32_t> OpenLoopLatencyCollector::report(uint32_t from_type, uint32_t to_type) {
    if (timestamps_in_us.find(from_type) == timestamps_in_us.end() ||
        timestamps_in_us.find(to_type) == timestamps_in_us.end()) {
        std::cerr << "unknown from_type:" << from_type << "or unknown to_type" << to_type << std::endl;
        return {0.0,0.0,0};
    }

    uint32_t count = timestamps_in_us.size();
    double sum = 0.0,square_sum = 0.0;
    double avg = 0.0, stddev = 0.0;
    for (uint32_t i=0;i<count;i++) {
        sum += (timestamps_in_us.at(to_type)[i] - timestamps_in_us.at(from_type)[i]);
    }
    avg = sum/count;
    for (uint32_t i=0;i<count;i++) {
        square_sum += (timestamps_in_us.at(to_type)[i] - timestamps_in_us.at(from_type)[i] - avg) *
                      (timestamps_in_us.at(to_type)[i] - timestamps_in_us.at(from_type)[i] - avg);
    }
    stddev = sqrt(square_sum/(count+1));
    return std::make_tuple(avg,stddev,count);
}

std::unique_ptr<OpenLoopLatencyCollector> OpenLoopLatencyCollector::create_server(
        uint32_t max_ids,
        const std::vector<uint32_t>& type_set,
        const std::function<bool(const std::map<uint32_t,uint32_t>&)>& udp_acks_collected,
        uint16_t udp_port) {
    return std::make_unique<OpenLoopLatencyCollector>(max_ids,type_set,udp_acks_collected,udp_port);
}

#ifdef ENABLE_EVALUATION
TimestampLogger::TimestampLogger() {
    // load the tag filters...
    if (hasCustomizedConfKey(CASCADE_TIMESTAMP_TAG_FILTER)) {
        std::istringstream f(getConfString(CASCADE_TIMESTAMP_TAG_FILTER));
        std::string s;
        while(getline(f,s,',')) {
            this->tag_enabler.emplace(std::stoul(s));
        }
    }
    pthread_spin_init(&lck,PTHREAD_PROCESS_PRIVATE);
    _log.reserve(65536);
}

void TimestampLogger::instance_log(uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t ts_ns, uint64_t extra) {
    if (tag_enabler.find(tag) != tag_enabler.cend()) {
        pthread_spin_lock(&lck);
        _log.emplace_back(tag,node_id,msg_id,ts_ns,extra);
        pthread_spin_unlock(&lck);
    }
}

void TimestampLogger::instance_flush(const std::string& filename, bool clear) {
    pthread_spin_lock(&lck);
    std::ofstream outfile(filename);
    for (auto& ent: this->_log) {
        outfile << std::get<0>(ent) << " "
                << std::get<1>(ent) << " "
                << std::get<2>(ent) << " "
                << std::get<3>(ent) << " "
                << std::get<4>(ent) << std::endl;
    }
    outfile.close();
    if (clear) {
        _log.clear();
    }
    pthread_spin_unlock(&lck);
}

void TimestampLogger::instance_clear() {
    pthread_spin_lock(&lck);
    _log.clear();
    pthread_spin_unlock(&lck);
}

TimestampLogger TimestampLogger::_tl{};
#endif

__attribute__((visibility("hidden"))) int64_t precedence(char op) {
    if (op == '+' || op == '-') return 1;
    if (op == '*' || op == '/') return 2;
    return 0;
}

__attribute__((visibility("hidden"))) int64_t applyOp(int64_t a, int64_t b, char op) {
    switch (op) {
        case '+': return a + b;
        case '-': return a - b;
        case '*': return a * b;
        case '/': return a / b;
    }
    return 0;
}

/**
 * This code is generated by GPT4 and modified slightly
 */
int64_t evaluate_arithmetic_expression(const std::string &expression) {
    std::stack<int64_t> values;
    std::stack<char> ops;

    for (size_t i = 0; i < expression.length(); i++) {
        if (expression[i] == ' ') continue;

        else if (expression[i] == '(') {
            ops.push(expression[i]);
        }

        else if (isdigit(expression[i])) {
            int64_t val = 0;
            while (i < expression.length() && isdigit(expression[i])) {
                val = (val * 10) + (expression[i] - '0');
                i++;
            }
            values.push(val);
            i--;
        }

        else if (expression[i] == ')') {
            while (!ops.empty() && ops.top() != '(') {
                int64_t val2 = values.top(); values.pop();
                int64_t val1 = values.top(); values.pop();

                char op = ops.top(); ops.pop();

                values.push(applyOp(val1, val2, op));
            }
            if (!ops.empty()) ops.pop();
        }

        else {
            while (!ops.empty() && precedence(ops.top()) >= precedence(expression[i])) {
                int64_t val2 = values.top(); values.pop();
                int64_t val1 = values.top(); values.pop();

                char op = ops.top(); ops.pop();

                values.push(applyOp(val1, val2, op));
            }
            ops.push(expression[i]);
        }
    }

    while (!ops.empty()) {
        int64_t val2 = values.top(); values.pop();
        int64_t val1 = values.top(); values.pop();

        char op = ops.top(); ops.pop();

        values.push(applyOp(val1, val2, op));
    }

    return values.top();
}

}
}


