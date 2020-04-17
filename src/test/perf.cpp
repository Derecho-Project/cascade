#include <iostream>
#include <vector>
#include <memory>
#include <derecho/core/derecho.hpp>
#include <derecho/utils/logger.hpp>
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>

using namespace derecho::cascade;
using derecho::ExternalClientCaller;

using VCS = VolatileCascadeStore<uint64_t,Object,&Object::IK,&Object::IV>;
using PCS = PersistentCascadeStore<uint64_t,Object,&Object::IK,&Object::IV,ST_FILE>;

int do_server() {
    dbg_default_info("Starting cascade server.");

    /** 1 - group building blocks*/
    derecho::CallbackSet callback_set {
        nullptr,    // delivery callback
        nullptr,    // local persistence callback
        nullptr     // global persistence callback
    };
    derecho::SubgroupInfo si {
        derecho::DefaultSubgroupAllocator({
            {std::type_index(typeid(VCS)),
             derecho::one_subgroup_policy(derecho::flexible_even_shards("VCS"))},
            {std::type_index(typeid(PCS)),
             derecho::one_subgroup_policy(derecho::flexible_even_shards("PCS"))}
        })
    };
    auto vcs_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCS>(
            [](derecho::subgroup_id_t sid,
               const uint32_t shard_num,
               const uint64_t& key,
               const Object& value){
                dbg_default_info("Volatile watcher is called with\n\tsubgroup id = {},\n\tshard number = {},\n\tkey = {},\n\tvalue = [hidden].", sid, shard_num, key);
            });
    };
    auto pcs_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCS>(
            pr,
            [](derecho::subgroup_id_t sid,
               const uint32_t shard_num,
               const uint64_t& key,
               const Object& value){
                dbg_default_info("Persistent watcher is called with\n\tsubgroup id = {},\n\tshard number = {},\n\tkey = {},\n\tvalue = [hidden].", sid, shard_num, key);
            });
    };
    /** 2 - create group */
    derecho::Group<VCS,PCS> group(callback_set,si,nullptr/*deserialization manager*/,
                                  std::vector<derecho::view_upcall_t>{},
                                  vcs_factory,pcs_factory);
    while (true) {};
    // group.barrier_sync();
    // group.leave();
    return 0;
}

int do_client(char** args) {

    struct timespec t_start, t_end;
    const char* test_type = args[0];
    const uint num_messagess = std::stoi(args[1]);
    const uint is_persistent = std::stoi(args[2]);

    if (strcmp(test_type,"write_throughput") != 0) {
        std::cout << "TODO:" << test_type << " not supported yet." << std::endl;
        return 0;
    }

    /** 1 - create external client group*/
    derecho::ExternalGroup<VCS,PCS> group;
    
    uint64_t msg_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);

    if (is_persistent) {
        if (derecho::hasCustomizedConfKey("SUBGROUP/PCS/max_payload_size")) {
            msg_size = derecho::getConfUInt64("SUBGROUP/PCS/max_payload_size") - 128;
        }
        char* bbuf = (char*)malloc(msg_size);
        bzero(bbuf, msg_size);

        ExternalClientCaller<PCS,std::remove_reference<decltype(group)>::type>& pcs_ec = group.get_subgroup_caller<PCS>();

        clock_gettime(CLOCK_REALTIME, &t_start);
        for(uint i = 0; i < num_messagess-1; i++) {
            Object o(i,Blob(bbuf, msg_size));
            pcs_ec.p2p_send<RPC_NAME(put)>(0,o);
        }
        Object o(num_messagess-1,Blob(bbuf, msg_size));
        auto result = pcs_ec.p2p_send<RPC_NAME(put)>(0,o);
        auto reply = result.get().get(0);
        clock_gettime(CLOCK_REALTIME, &t_end);
        std::cout << "put finished with timestamp=" << std::get<0>(reply)
                << ",version=" << std::get<1>(reply) << std::endl;
        free(bbuf);
    } else {
        if (derecho::hasCustomizedConfKey("SUBGROUP/VCS/max_payload_size")) {
            msg_size = derecho::getConfUInt64("SUBGROUP/VCS/max_payload_size") - 128;
        }
        char* bbuf = (char*)malloc(msg_size);
        bzero(bbuf, msg_size);

        ExternalClientCaller<VCS,std::remove_reference<decltype(group)>::type>& vcs_ec = group.get_subgroup_caller<VCS>();

        clock_gettime(CLOCK_REALTIME, &t_start);
        for(uint i = 0; i < num_messagess-1; i++) {
            Object o(i,Blob(bbuf, msg_size));
            vcs_ec.p2p_send<RPC_NAME(put)>(0,o);
        }
        Object o(num_messagess-1,Blob(bbuf, msg_size));
        auto result = vcs_ec.p2p_send<RPC_NAME(put)>(0,o);
        auto reply = result.get().get(0);
        clock_gettime(CLOCK_REALTIME, &t_end);
        std::cout << "put finished with version=" << std::get<0>(reply)
                << ",timestamp=" << std::get<1>(reply) << std::endl;
        free(bbuf);
    }

    int64_t nsec = ((int64_t)t_end.tv_sec - t_start.tv_sec) * 1000000000 + t_end.tv_nsec - t_start.tv_nsec;
    double msec = (double)nsec / 1000000;
    double thp_gbps = ((double)num_messagess * msg_size * 8) / nsec;
    double thp_ops = ((double)num_messagess * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "Gbit/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;

    return 0;
}

void print_help(std::ostream& os,const char* bin) {
    os << "USAGE:" << bin << " [derecho-config-list --] <client|server> args..." << std::endl;
    os << "    client args: <test_type> <num_messages> <is_persistent>" << std::endl;
    os << "        test_type := [write|read]_[throughput|latency]" << std::endl;
    os << "    server args: N/A" << std::endl;
}

int index_of_first_arg(int argc, char** argv) {
    int idx = 1;
    int i = 2;

    while (i<argc) {
        if (strcmp("--",argv[i]) == 0) {
            idx = i + 1;
            break;
        }
        i++;
    }
    return idx;
}

int main(int argc, char** argv) {
    /** initialize the parameters */
    derecho::Conf::initialize(argc,argv);

    /** check parameters */
    int first_arg_idx = index_of_first_arg(argc,argv);
    if (first_arg_idx >= argc) {
        print_help(std::cout,argv[0]);
        return 0;
    }

    if (strcmp(argv[first_arg_idx],"server") == 0) {
        return do_server();
    } else if (strcmp(argv[first_arg_idx],"client") == 0) {
        if ((argc - first_arg_idx) != 4 ) {
            std::cerr << "Invalid client args." << std::endl;
            print_help(std::cerr,argv[0]);
            return -1;
        }
        // passing <test_type> <num_messages> <is_persistent>
        return do_client(&argv[first_arg_idx+1]);
    } else {
        std::cerr << "Error: unknown arg: " << argv[first_arg_idx] << std::endl;
        print_help(std::cerr,argv[0]);
        return -1;
    }
}
