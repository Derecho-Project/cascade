#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include "cnn_classifier_dpl.hpp"

using namespace derecho::cascade;

auto parse_file_list(const char* type, const char* files) {
    std::vector<VolatileCascadeStoreWithStringKey::ObjectType> vec;
    std::istringstream fs(files);
    std::string f;
    size_t key = 0;
    while(std::getline(fs,f,':')){
        vec.push_back(get_photo_object(type,std::to_string(key).c_str(),f.c_str()));
        key ++;
    }
    return vec;
}

#ifdef EVALUATION
#define BUFSIZE (256)
void collect_time(uint16_t udp_port, size_t num_messages, uint64_t* timestamps, uint64_t* inference_us, uint64_t* put_us) {
    struct sockaddr_in serveraddr,clientaddr;
    char buf[BUFSIZE];
    //STEP 1: start UDP channel
  	int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  	if (sockfd < 0) {
        std::cerr << "ERROR opening socket" << std::endl;
        return;
    }
  	int optval = 1;
  	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval , sizeof(int));
    //STEP 2: waiting for UDP message
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(udp_port);
    if (bind(sockfd, (struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0) {
        std::cerr << "Fail to bind udp port:" << udp_port << std::endl;
    	return;
    }
    socklen_t clientlen = sizeof(clientaddr);
    size_t cnt = 0;
    while (cnt < num_messages) {
        ssize_t nrecv = recvfrom(sockfd,buf,BUFSIZE,0,(struct sockaddr *) &clientaddr,&clientlen);
        if (nrecv < 0) {
            std::cerr << "Fail to recv udp package." << std::endl;
            return;
        }
        CloseLoopReport *clr = reinterpret_cast<CloseLoopReport*>(buf);
        timestamps[clr->photo_id] = get_time();
        inference_us[clr->photo_id] = clr->inference_us;
        put_us[clr->photo_id] = clr->put_us;
        cnt ++;
    }
    //STEP 3: finish 
    close(sockfd);
}
#endif

/**
 * The cnn classifier client post photos to cascade to be processed by the cnn classifier data path logic.
 */
int main(int argc, char** argv) {
    const char* HELP_INFO = "--(t)ype <pet|flower> --(f)iles <file1:file2:file3...>\n"
                            "--(n)um_messages <number of messages, default to 100>\n"
                            "--(i)nterval_us <message interval in microseconds, default to 1000000 (1s)>\n"
#ifdef EVALUATION
                            "--(u)dp_port <UDP port for report server. For evaluation only, default=54321>\n"
#endif
                            "--(h)elp";
    int c;
    static struct option long_options[] = {
        {"files",   required_argument,  0,  'f'},
        {"type",    required_argument,  0,  't'},
        {"num_messages",    required_argument,  0,  'n'},
        {"interval_us",     required_argument,  0,  'i'},
#ifdef EVALUATION
        {"udp_port",		required_argument,  0,  'u'},
#endif
        {"help",    no_argument,        0,  'h'},
        {0,0,0,0}
    };
    const char* files = nullptr;
    const char* type = nullptr;
    size_t num_messages = 100;
    size_t interval_us = 1000000;
#ifdef EVALUATION
    uint16_t udp_port = 54321;
#endif
    bool print_help = false;

    while(true){
        int option_index = 0;
        c = getopt_long(argc,argv,"f:t:n:i:h",long_options,&option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'f':
            files = optarg;
            break;
        case 't':
            type = optarg;
            break;
        case 'n':
            num_messages = std::stol(optarg);
            break;
        case 'i':
            interval_us = std::stol(optarg);
            break;
#ifdef EVALUATION
        case 'u':
            udp_port = std::stol(optarg);
            break;
#endif
        case 'h':
            print_help = true;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            break;
        }
    }

    if (!(files && type)) {
        if (!print_help) {
            std::cout << "Invalid argument." << std::endl;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            return -1;
        }
    } else {
        size_t window_size = derecho::getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE);
        auto vec_photos = parse_file_list(type,files);
        const size_t vec_size = vec_photos.size();
        ServiceClientAPI capi;
        std::list<derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>>> results;
        // TODO: change this to asynchronous send.
#ifdef EVALUATION
        uint64_t send_message_ts[num_messages];
        uint64_t before_send_message_ts[num_messages];
        uint64_t before_query_ts[num_messages];
        uint64_t after_query_ts[num_messages];
        uint64_t close_loop_ts[num_messages];
        uint64_t inference_us[num_messages]; // inference time cost in microsecond
        uint64_t put_us[num_messages]; // "put to pers" time cost in microsecond
        std::thread cl_thread(collect_time, udp_port, num_messages, (uint64_t*)close_loop_ts, (uint64_t*)inference_us, (uint64_t*)put_us);
#endif
        uint64_t prev_us = 0, now_us;
        size_t num_replied = 0;
        for(size_t i=0;i<num_messages;i++) {
#ifdef EVALUATION
            before_send_message_ts[i] = get_time();
#endif
            /*
             * key = [i%vec_size]
             * version = i/vec_size
             * i = stoi(key) + version*vec_size;
             */
            FrameData *fd = reinterpret_cast<FrameData*>(vec_photos.at(i%vec_size).blob.bytes);
            fd->photo_id = i;

            if (results.size() >= window_size) {
                // window_size is full, read one reply first.
#ifdef EVALUATION
                before_query_ts[num_replied] = get_time();
#endif
                for (auto& reply_future:results.front().get()) {
                    auto reply = reply_future.second.get();
                    std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                              << ",ts_us:" << std::get<1>(reply) << std::endl;
                }
#ifdef EVALUATION
                after_query_ts[num_replied] = get_time();
#endif
                results.pop_front();
                num_replied ++;
            }
            now_us = get_time()/1000;
            if ((now_us - prev_us) < interval_us) {
                usleep(prev_us+interval_us-now_us);
            }
	    prev_us = get_time()/1000;
            results.emplace_back(std::move(capi.template put<VolatileCascadeStoreWithStringKey>(vec_photos.at(i%vec_size), 0, 0)));
#ifdef EVALUATION
            send_message_ts[i] = get_time();
#endif
        }

        while(results.size() > 0) {
#ifdef EVALUATION
            before_query_ts[num_replied] = get_time();
#endif
            for (auto& reply_future:results.front().get()) {
                auto reply = reply_future.second.get();
                std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                          << ",ts_us:" << std::get<1>(reply) << std::endl;
            }
#ifdef EVALUATION
            after_query_ts[num_replied] = get_time();
#endif
            results.pop_front();
            num_replied ++;
        }

#ifdef EVALUATION
        cl_thread.join();
        // TODO: evaluate using the data
        uint64_t max_recv_ts = 0;
        uint64_t latencies[num_messages];
        double avg_lat = 0.0;
        double lat_std = 0.0;
        double avg_infer_lat = 0.0;
        double infer_lat_std = 0.0;
        double avg_put_lat = 0.0;
        double put_lat_std = 0.0;
        for (size_t i=0;i<num_messages;i++) {
            if (max_recv_ts < close_loop_ts[i]) {
                max_recv_ts = close_loop_ts[i];
                latencies[i] = (close_loop_ts[i] - send_message_ts[i]);
                avg_lat += (double)latencies[i]/num_messages;
                avg_infer_lat += (double)inference_us[i]/num_messages;
                avg_put_lat += (double)put_us[i]/num_messages;
            }
            std::cout << "[" << i << "] " << (send_message_ts[i] - before_send_message_ts[i])/1000000 << "," 
                                          << (before_query_ts[i] - send_message_ts[i])/1000000 << "," 
                                          << (after_query_ts[i]  - before_query_ts[i])/1000000 << " | " 
                                          << (close_loop_ts[i]   - before_send_message_ts[i])/1000000 << std::endl;
        }
        double span_ns = (double)(max_recv_ts - before_send_message_ts[0]);
        std::cout << "Timespan:\t" << span_ns/1e6 << " milliseconds." << std::endl;
        // Thoughput
        std::cout << "Throughput:\t" << (double)(num_messages*1e9)/span_ns << " ops." << std::endl;
        // Latency
        double lat_ssum = 0.0;
        double ilat_ssum = 0.0;
        double plat_ssum = 0.0;
        for(size_t i=0;i<num_messages;i++) {
            lat_ssum += (latencies[i]-avg_lat)*(latencies[i] - avg_lat);
            ilat_ssum += (inference_us[i]-avg_infer_lat)*(inference_us[i]-avg_infer_lat);
            plat_ssum += (put_us[i]-avg_put_lat)*(put_us[i]-avg_put_lat);
        }
        lat_std = sqrt(lat_ssum/(num_messages-1));
        infer_lat_std = sqrt(ilat_ssum/(num_messages-1));
        put_lat_std = sqrt(plat_ssum/(num_messages-1));

        std::cout << "Latency:\t" << avg_lat/1e6 << " ms, standard deviation: " << lat_std/1e6 << " ms."  << std::endl;
        std::cout << "Inference Latency:\t" << avg_infer_lat/1e3 << " ms, standard deviation: " << infer_lat_std/1e3 << " ms."  << std::endl;
        std::cout << "Put Latency:\t" << avg_put_lat/1e3 << " ms, standard deviation: " << put_lat_std/1e3 << " ms."  << std::endl;
#endif
    }

    return 0;
}
