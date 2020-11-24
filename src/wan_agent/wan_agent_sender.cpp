#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <string.h>
#include <thread>
#include <time.h>
#include <unistd.h>

#include <wan_agent/wan_agent.hpp>

using std::cout;
using std::endl;

static uint64_t now_us() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return (tv.tv_sec * 1000000 + tv.tv_nsec / 1000);
}

static void print_statistics(uint64_t* time_keeper, std::size_t number_of_messages, std::size_t size_of_message) {
    double sum_min = 0.0f, sum_max = 0.0f, sum_median = 0.0f;
    std::size_t i;
    uint64_t* latencies = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * number_of_messages * 3));
    for(i = 0; i < number_of_messages; i++) {
        latencies[i * 3] = (time_keeper[i * 4 + 1] - time_keeper[i * 4]);
        latencies[i * 3 + 1] = (time_keeper[i * 4 + 2] - time_keeper[i * 4]);
        latencies[i * 3 + 2] = (time_keeper[i * 4 + 3] - time_keeper[i * 4]);
        sum_min += latencies[i * 3];
        sum_median += latencies[i * 3 + 1];
        sum_max += latencies[i * 3 + 2];
    }
    /*
    std::cout << "one stability frontier latency = " << sum_min/number_of_messages << " us." << std::endl;
    std::cout << "majority stability frontier latency = " << sum_median/number_of_messages << " us." << std::endl;
    std::cout << "global stability frontier latency = " << sum_max/number_of_messages << " us." << std::endl;
    // list all latencies:
    std::cout << "seqno\tone\tmajority\tglobal" << std::endl;
    for (i=0;i<number_of_messages;i++) {
        std::cout << i << "\t"
                  << latencies[3*i] << "\t"
                  << latencies[3*i+1] << "\t"
                  << latencies[3*i+2] << std::endl;
    }
     */
    std::cout << size_of_message / 1024 << "," << sum_min / number_of_messages / 1000 << "," << sum_median / number_of_messages / 1000 << "," << sum_max / number_of_messages / 1000 << "," << (size_of_message * number_of_messages / 1024.0 * 1000000) / ((time_keeper[(number_of_messages - 1) * 4 + 1] - time_keeper[0])) << "," << (size_of_message * number_of_messages / 1024.0 * 1000000) / ((time_keeper[(number_of_messages - 1) * 4 + 2] - time_keeper[0])) << "," << (size_of_message * number_of_messages / 1024.0 * 1000000) / ((time_keeper[(number_of_messages - 1) * 4 + 3] - time_keeper[0])) << std::endl;
    free(latencies);
}

static void print_help(const char* cmd) {
    std::cout << "Usage: " << cmd << " -c <json_config_file>"
              << " [-s(sender)]"
              << " [-i interval(micro)]"
              << " [-m message_size(in bytes)]"
              << " [-n number_of_messages]"
              << std::endl;
}

#define MAX_SEND_BUFFER_SIZE (102400)
#define SLEEP_GRANULARITY_US (50)

int main(int argc, char** argv) {
    int opt;
    std::string json_config;
    std::size_t send_interval_us = 0;
    std::size_t message_size = 0;
    std::size_t number_of_messages = 0;

    while((opt = getopt(argc, argv, "c:i:m:n:")) != -1) {
        switch(opt) {
            case 'c':
                json_config = optarg;
                break;
            case 'i':
                send_interval_us = static_cast<std::size_t>(std::stol(optarg));
                break;
            case 'm':
                message_size = static_cast<std::size_t>(std::stol(optarg));
                break;
            case 'n':
                number_of_messages = static_cast<std::size_t>(std::stol(optarg));
                break;
            default:
                print_help(argv[0]);
                return -1;
        }
    }

    if(json_config.size() == 0) {
        print_help(argv[0]);
        return -1;
    }

    std::ifstream json_file(json_config);
    nlohmann::json conf;
    json_file >> conf;

    uint64_t* time_keeper = nullptr;
    std::atomic<bool> all_received(false);

    std::cout << "number_of_messages = " << number_of_messages << std::endl;
    std::cout << "message_size = " << message_size << std::endl;
    std::cout << "intervals = " << send_interval_us << " us" << std::endl;
    if(number_of_messages <= 0 || message_size <= 0 || send_interval_us < 0) {
        std::cerr << "invalid argument." << std::endl;
        return -1;
    }

    time_keeper = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * 4 * number_of_messages));
    if(time_keeper == nullptr) {
        throw std::runtime_error("failed to allocate memory for time keeper");
    } else {
        // touch the memory
        bzero(static_cast<void*>(time_keeper), sizeof(uint64_t) * 4 * number_of_messages);
    }

    std::cout << "time_keeper:" << time_keeper << std::endl;

    wan_agent::PredicateLambda pl = [&](const std::map<uint32_t, uint64_t>& table) {
        if(time_keeper != nullptr) {
            uint64_t min, max, median;
            std::vector<uint64_t> v;
            for(auto& site : conf[WAN_AGENT_CONF_SERVER_SITES]) {
                uint32_t sid = site[WAN_AGENT_CONF_SITES_ID];
                v.push_back(table.at(sid));
            }
            std::sort(v.begin(), v.end());
            min = v[0];
            median = v[v.size() / 2];
            max = v[v.size() - 1];
            uint64_t now = now_us();
            // global
            while(min > 0 && time_keeper[min * 4 - 1] == 0) {
                time_keeper[min * 4 - 1] = now;
                min--;
            }
            // majority
            while(median > 0 && time_keeper[median * 4 - 2] == 0) {
                time_keeper[median * 4 - 2] = now;
                median--;
            }
            // one
            while(max > 0 && time_keeper[max * 4 - 3] == 0) {
                time_keeper[max * 4 - 3] = now;
                max--;
            }
        }
        if(time_keeper[number_of_messages * 4 - 1] != 0) {
            all_received.store(true);
        }
    };

    wan_agent::WanAgentSender wan_agent_sender(conf, pl);

    std::cout << "Press ENTER to send a message." << std::endl;
    std::cin.get();

    // prepare the sender buffer
    char* payload = static_cast<char*>(malloc(MAX_SEND_BUFFER_SIZE));
    if(payload == nullptr) {
        throw std::runtime_error("failed to allocate payload memory");
    }
    for(std::size_t i = 0; i < message_size; i++) {
        payload[i] = '0' + (i % 10);
    }

    std::cout << "payload size is " << strlen(payload) << std::endl;
    // send ...
    for(uint64_t seq = 0; seq < number_of_messages; seq++) {
        time_keeper[seq * 4] = now_us();
        wan_agent_sender.send(payload, message_size);
        //            std::cout << "send a message with size = " << message_size << std::endl;
        while(now_us() < (time_keeper[seq * 4] + send_interval_us)) {
            std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_GRANULARITY_US));
        }
    }
    std::cout << "Done send messages, waiting for all the reports." << std::endl;
    // wait till end
    while(all_received == false) {
        std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_GRANULARITY_US));
    }
    std::cout << "Send finished." << std::endl;
    print_statistics(time_keeper, number_of_messages, message_size);

    std::cout << "Press ENTER to kill." << std::endl;
    std::cin.get();
    wan_agent_sender.shutdown_and_wait();

    free(payload);
    free(time_keeper);
}