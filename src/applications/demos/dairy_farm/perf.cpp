#include <cascade/service_client_api.hpp>
#include <experimental/bits/fs_fwd.h>
#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <rpc/server.h>
#include <rpc/client.h>
#include <rpc/rpc_error.h>
#include <memory>
#include <vector>
#include <map>
#include <demo_udl.hpp>

#if __GNUC__ >= 9
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

#include <derecho/utils/logger.hpp>

using namespace derecho::cascade;

#define PERFTEST_PORT   (18721)

static void print_help(const std::string& cmd) {
    std::cout << "Usage:" << std::endl;
    std::cout << "Run as a perf server:\n"
              << "\t" << cmd << " server <frame_path> [ip:port, default to 127.0.0.1:" << PERFTEST_PORT << "]"
              << std::endl;
    std::cout << "Run as a perf client:\n"
              << "\t" << cmd << " client <trigger_put|put_and_forget> <pathname> <max rate> <duration in secs> <list of concurrent clients>"
              << std::endl;
    return;
}

static auto load_frames(const std::string& frame_path) {
    std::vector<ObjectWithStringKey> frames;
#if __GNUC__ >= 9
    for (const auto & entry : std::filesystem::directory_iterator{frame_path}) {
#else
    for (const auto & entry : std::experimental::filesystem::directory_iterator{frame_path}) {
#endif
        std::cout << entry.path() << std::endl;
#if __GNUC__ >= 9
        if (entry.is_regular_file()) {
#else
        if (std::experimental::filesystem::is_regular_file(entry)) {
#endif
            frames.emplace_back(
                std::move(
                    get_photo_object(
                        entry.path().filename().c_str(),
                        entry.path().c_str())));
        }
    }
    return frames;
}

static int do_server(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Invalid arguments" << std::endl;
        return -1;
    }
    std::string frame_path(argv[2]);
    std::string localhost("127.0.0.1");
    uint16_t port = PERFTEST_PORT;
    if (argc >=4) {
        auto colpos = std::string(argv[3]).find(':');
        if (colpos == std::string::npos) {
            localhost = argv[3];
        } else {
            localhost = std::string(argv[3],0,colpos);
            port = static_cast<uint16_t>(std::stoul(argv[3]+colpos+1));
        }
    }
    uint64_t payload_size = derecho::getConfUInt64(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
    // 1 - load frames to prepare the workload
    auto frames = load_frames(frame_path);
    ::rpc::server rpc_server(localhost,port);
    ServiceClientAPI capi;
    // 2 - create rpclib server, waiting for execution
    rpc_server.bind("perf",[&frames,&capi,payload_size](
        const std::string& pathname,
        bool               is_trigger,
        uint64_t           max_operation_per_second,
        int64_t            start_sec,
        uint64_t           duration_sec) {
        uint64_t next_ns = (start_sec)*1e9;
        uint64_t stop_ns = next_ns + duration_sec*1e9;
        uint64_t interval_ns = 1e9/max_operation_per_second;
#ifdef ENABLE_EVALUATION
        uint64_t message_id = capi.get_my_id()*1000000000;
#endif
        // - send frames at given rate
        while (next_ns <= stop_ns) {
            int64_t sleep_us = (next_ns - static_cast<int64_t>(get_walltime()))/1e3;
            if (sleep_us > 1) {
                usleep(sleep_us);
            }
            next_ns += interval_ns;
            // - send a frame.
            std::size_t object_index = get_walltime()%frames.size();
            while (frames.at(object_index).bytes_size() > payload_size) {
                std::cout << "object-" << object_index << " has " 
                          << frames.at(object_index).bytes_size() << " bytes,"
                          << " which is too large for maximum p2p request payload size ("
                          << payload_size << "). Skip it." << std::endl;
                object_index = get_walltime()%frames.size();
            }
#ifdef ENABLE_EVALUATION
            if (std::is_base_of<IHasMessageID,decltype(frames.at(object_index))>::value) {
                frames.at(object_index).set_message_id(message_id++);
            }
#endif
            capi.trigger_put(frames.at(object_index));
#ifdef ENABLE_EVALUATION
            global_timestamp_logger.log(TLT_DAIRYFARMDEMO(0),capi.get_my_id(),message_id,get_walltime());
#endif
        }
        return true;
    });
#ifdef ENABLE_EVALUATION
    rpc_server.bind("flush_timestamp_log", [&capi](const std::string& output_filename,bool flush_server) {
        global_timestamp_logger.flush(output_filename);

        if (flush_server) {
            // collect the object pools
            std::set<std::string> object_pools;
            for (const auto& dfg:DataFlowGraph::get_data_flow_graphs()) {
                if (dfg.id == "8ac4c636-9d92-11eb-9dbc-0242ac110002") {
                    // only check dairy farm demo.
                    for (const auto& vertex:dfg.vertices) {
                        object_pools.emplace(vertex.first);
                        for (const auto& edge:vertex.second.edges) {
                            for (const auto& op:edge.second) {
                                object_pools.emplace(op.first);
                            }
                        }
                    }
                }
            }
            // collect the type and subgroups.
            std::set<std::tuple<uint32_t,uint32_t>> subgroups;
            for (const auto& op:object_pools) {
                auto opm = capi.find_object_pool(op);
                subgroups.emplace(opm.subgroup_type_index,opm.subgroup_index);
                dbg_default_trace("Collected subgroup: type:{} index:{}.",opm.subgroup_type_index,opm.subgroup_index);
            }
            // do flush_timestamp_log
            // TODO: this should be done in a more elegant way using template expansion 
#define DUMP_TIMPSTAMP(subgroup_type) \
                    { \
                        uint32_t num_shard = capi.template get_number_of_shards<subgroup_type>(std::get<1>(subgroup)); \
                        while(num_shard>0) { \
                            auto result = capi.template dump_timestamp<subgroup_type>(output_filename,std::get<1>(subgroup),--num_shard); \
                            result.get(); \
                        } \
                    }
            for (const auto& subgroup:subgroups) {
                switch(std::get<0>(subgroup)) {
                case 1:
                    DUMP_TIMPSTAMP(VolatileCascadeStoreWithStringKey);
                    break;
                case 2:
                    DUMP_TIMPSTAMP(PersistentCascadeStoreWithStringKey);
                    break;
                case 3:
                    DUMP_TIMPSTAMP(TriggerCascadeNoStoreWithStringKey);
                    break;
                default:
                    std::cerr << "Invalid subgroup type index:" << std::get<0>(subgroup) << std::endl;
                }
                dbg_default_trace("dump_timestamp type:{} index:{}.", std::get<0>(subgroup), std::get<1>(subgroup));
            }
        }

        return true;
    });
#endif //ENABLE_EVALUATION
    // 3 - run.
    rpc_server.async_run(1);
    std::cout << "Press ENTER to stop" << std::endl;
    std::cin.get();
    rpc_server.stop();
    return 0;
}

static bool check_rpc_futures(std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>>&& futures) {
    bool ret = true;
    for (auto& kv:futures) {
        try{
            ret = kv.second.get().as<bool>();
            std::cout << "returned:" << ret << std::endl;
        } catch (::rpc::rpc_error& rpce) {
            dbg_default_warn("perf server {}:{} throws an exception. function:{}, error:{}",
                             kv.first.first,
                             kv.first.second,
                             rpce.get_function_name(),
                             rpce.get_error().as<std::string>());
            ret = false;
        } catch (...) {
            dbg_default_warn("perf server {}:{} throws unknown exception.",
                             kv.first.first, kv.first.second);
            return false;
        }
    }
    return ret;
}

static int do_client(int argc, char** argv) {
    bool trigger_mode = (std::string("trigger_put") == argv[2]);
    std::string pathname(argv[3]);
    uint64_t max_rate_ops = std::stoul(argv[4]);
    uint64_t duration_sec = std::stoul(argv[5]);

    std::vector<std::pair<std::string,uint16_t>> perf_servers;
    for (int i=6;i<argc;i++) {
        std::string ip;
        uint16_t port;
        auto colpos = std::string(argv[i]).find(':');
        if (colpos == std::string::npos) {
            ip = argv[i];
            port = PERFTEST_PORT;
        } else {
            ip = std::string(argv[i],0,colpos);
            port = static_cast<uint16_t>(std::stoul(argv[i]+colpos+1));
        }
        perf_servers.emplace_back(ip,port);
    }

    int64_t start_sec = get_walltime()/1000000000 + 5; // start about 5 seconds later.
   
    std::map<std::pair<std::string,uint16_t>,std::unique_ptr<::rpc::client>> connections;
    std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for (auto& s:perf_servers) {
        if (connections.find(s) != connections.end()) {
            connections.erase(s);
        }
        connections.emplace(s,std::make_unique<::rpc::client>(s.first,s.second));
    }
    // send perf
    for (auto& kv:connections) {
        futures.emplace(kv.first,kv.second->async_call("perf",
                                                       pathname,
                                                       trigger_mode,
                                                       max_rate_ops,
                                                       start_sec,
                                                       duration_sec));
    }
    check_rpc_futures(std::move(futures));
    usleep(1000000); // sleep for 1 second.
    // dump timestamps
    bool is_first_client = true;
    for (auto& kv:connections) {
        futures.emplace(kv.first,kv.second->async_call("flush_timestamp_log","perf.log",is_first_client));
        is_first_client = false;
    }
    check_rpc_futures(std::move(futures));
    // destruct clients.
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        print_help(argv[0]);
        return -1;
    }
    if (std::string(argv[1]) == "server") {
        return do_server(argc,argv);
    } else if (std::string(argv[1]) == "client") {
        return do_client(argc,argv);
    } else {
        print_help(argv[0]);
    }

    return -1;
}
