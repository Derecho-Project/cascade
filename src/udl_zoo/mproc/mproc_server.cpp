#include <cascade/mproc/mproc_ctxt_server.hpp>
#include <csignal>
#include <iostream>
#include <atomic>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <unistd.h>
#include <getopt.h>

#include "mproc_udl_server.hpp"

namespace fs = std::filesystem;

namespace derecho {
namespace cascade {

using json = nlohmann::json;

/**
 * @class GlobalStates
 * @brief global states
 */
class GlobalStates {
private:
    static std::atomic<bool> stop_flag; /// stop flag
public:
    /**
     * @fn stop()
     * @brief stop the program
     */
    static void stop() {
        stop_flag.store(true);
    }
    /**
     * @fn is_stopped()
     * @brief   Test the stop flag
     * @return  Stop flag
     */
    static bool is_stopped() {
        return stop_flag.load();
    }
    /**
     * @fn initialize()
     * @brief   Initialize global states.
     */
    static void initialize() {
        stop_flag = false;
    }
};

std::atomic<bool> GlobalStates::stop_flag;

/**
 * @fn signal_handler
 * @brief signal handler
 * @param signal    The signal received.
 */
void signal_handler(int signal) {
    std::cout << "Received signal: " << signal << std::endl;
    GlobalStates::stop();
}

const char* help_string = 
    "\t--app_cwd,-c The working directory, if not the current working directory.\n"
    "\t--objectpool_path,-p\n"
    "\t             The object pool path for the UDL.\n"
    "\t--udl_uuid,-u\n"
    "\t             The uuid of the UDL.\n"
    "\t--udl_conf,-U\n"
    "\t             The UDL configuration in json format.\n"
    "\t--execution_environment,-e\n"
    "\t             The execution environment, can be either process|docker.\n"
    "\t--execution_environmen_conf,-E\n"
    "\t             The execution environment configuration in json format.\n"
    "\t--statefulness,-s\n"
    "\t             The statefulness of the working directory, can be either stateful|stateless|singlethreaded.\n"
    "\t--number_threads,-t\n"
    "\t             The number of threads in the upcall thread pool.\n"
    "\t--edges,-o\n The output edges in json format.\n"
    "\t--rbkeys,-r\n"
    "\t             The ring buffer keys in json array format in the order of 1) object submit ring buffer, \n"
    "\t             2) context request ring buffer, 3) context response ring buffer. \n"
    "\t             For example: [2882339107,2882339108,2882339107]. \n"
    "\t             Please notice that HEX key format is not supported in the current json library.\n"
    "\t--help,-h    Print this message.\n";

/**
 * @fn void print_help(const char* command)
 * @brief   Print the help information
 * @param[in]   command     The name of the command
 */
__attribute__((visibility("hidden")))
void print_help(const char* command) {
    std::cout << "The mproc server" << std::endl;
    std::cout << "Usage:" << command << " [options]" << std::endl;
    std::cout << help_string << std::endl;
}

/**
 * @fn int mproc_server_main(int argc, char** argv)
 * @brief the entrypoint of a mproc server process.
 * @param[in]   argc
 * @param[out]  argv
 * @return  error code
 */
int mproc_server_main(int argc, char** argv) {
    // step 1 - collect options
    static struct option long_options[] = {
        {"app_cwd",                     required_argument,  0,  'c'},
        {"objectpool_path",             required_argument,  0,  'p'},
        {"udl_uuid",                    required_argument,  0,  'u'},
        {"udl_conf",                    required_argument,  0,  'U'},
        {"execution_environment",       required_argument,  0,  'e'},
        {"execution_environment_conf",  required_argument,  0,  'E'},
        {"statefulness",                required_argument,  0,  's'},
        {"number_threads",              required_argument,  0,  't'},
        {"edges",                       required_argument,  0,  'o'},
        {"rbkeys",                      required_argument,  0,  'r'},
        {"help",                        no_argument,        0,  'h'},
        {0,0,0,0}
    };

    mproc_udl_server_arg_t  mproc_server_args;

    while (true) {
        int option_index = 0;
        int c = getopt_long(argc,argv,"c:p:u:U:e:E:s:t:o:r:h",long_options,&option_index);

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'c':
            mproc_server_args.app_cwd = optarg;
            break;
        case 'p':
            mproc_server_args.objectpool_path = optarg;
            break;
        case 'u':
            mproc_server_args.udl_uuid = optarg;
            break;
        case 'U':
            mproc_server_args.udl_conf = json::parse(optarg);
            break;
        case 'e':
            if (std::string("process") == optarg) {
                mproc_server_args.exe_env = DataFlowGraph::VertexExecutionEnvironment::PROCESS;
            } else if (std::string("docker") == optarg) {
                mproc_server_args.exe_env = DataFlowGraph::VertexExecutionEnvironment::DOCKER;
            } else if (std::string("pthread") == optarg) {
                mproc_server_args.exe_env = DataFlowGraph::VertexExecutionEnvironment::PTHREAD;
            } else {
                std::cerr << "Unsupported execution environment:" << optarg << std::endl;
                std::cerr << "Only 'process' or 'docker' are supported." << std::endl;
                // for triggering an error.
                mproc_server_args.exe_env = DataFlowGraph::VertexExecutionEnvironment::UNKNOWN_EE;
            }
            break;
        case 'E':
            mproc_server_args.exe_env_conf = json::parse(optarg);
            break;
        case 's':
            if (std::string("singlethread") == optarg) {
                mproc_server_args.statefulness = DataFlowGraph::Statefulness::SINGLETHREADED;
            } else if (std::string("stateful") == optarg) {
                mproc_server_args.statefulness = DataFlowGraph::Statefulness::STATEFUL;
            } else if (std::string("stateless") == optarg) {
                mproc_server_args.statefulness = DataFlowGraph::Statefulness::STATELESS;
            } else {
                mproc_server_args.statefulness = DataFlowGraph::Statefulness::UNKNOWN_S;
            }
            break;
        case 't':
            mproc_server_args.num_threads = std::stoul(optarg);
            break;
        case 'o':
            mproc_server_args.edges = json::parse(optarg);
            break;
        case 'r':
            mproc_server_args.rbkeys = json::parse(optarg);
            break;
        case 'h':
            print_help(argv[0]);
            return 0;
        default:
            break;
        }
    }

    // step 3 - run server
    fs::current_path(fs::path(mproc_server_args.app_cwd));
    dbg_default_trace("Starting mproc server...");
    MProcUDLServer<CASCADE_SUBGROUP_TYPE_LIST>::run_server(mproc_server_args);
    dbg_default_trace("mproc server started.");
    // step 4 - steup kill handler.
    GlobalStates::initialize();
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    while(!GlobalStates::is_stopped());
    std::cout << "done." << std::endl;
    return 0;
}

}
}

int main(int argc, char** argv) {
    return derecho::cascade::mproc_server_main(argc,argv);
}
