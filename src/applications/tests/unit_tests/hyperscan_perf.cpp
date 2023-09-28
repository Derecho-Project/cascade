#include <getopt.h>
#include <cinttypes>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <cascade/utils.hpp>
#include <cstdio>
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <hs/hs.h>
#include <unistd.h>

/**
 * @file hyperscan_perf.cpp
 *
 * Hyperscan Performance Tester
 */

/**
 * @brief tag
 */
#define TLT_HYPERSCAN_START     5000001
#define xstr(a)                 str(a)
#define str(a)                  #a

/**
 * @brief Help string.
 */
const char* help_string = 
    "Hyperscan Performance Tester\n"
    "----------------------------\n"
    "Options:\n"
    "\t--(g)enerate-test-cases <num_entries>        generate test cases\n"
    "\t--(e)valuate <testcase file>                 evaluate test cases\n"
    "\t                                             per-scan latency is available by enabling timestamp tag:" xstr(TLT_HYPERSCAN_START) "\n"
    "\t                                             , by putting the following in derecho.cfg\n"
    "\t                                             /////////\n"
    "\t                                             [CASCADE]\n"
    "\t                                             timestamp_tag_enabler = " xstr(TLT_HYPERSCAN_START) "\n"
    "\t                                             ///////////////////////////////\n"
    "\t--(p)attern <regex>                          pattern for evaluation\n"
    "\t--(h)elp                                     help information\n"
    ;

/**
 * @brief generate test cases
 * Generate test cases with four prefixes used in the collision prediction applications
 *
 * @param[in]   num_test_cases  The number of test cases entries to generate.
 */
void generate_test_cases(uint32_t num_test_cases) {
    const static std::vector<std::string> prefix_list = {
        "/collision/tracking/cameras/little3_",
        "/collision/tracking/states/little3_",
        "/collision/tracking/agent_position/little3_7_",
        "/collision/prediction/agent_prediction/little3_42_",
    };
    
    std::srand(static_cast<unsigned>(derecho::cascade::get_time_ns()));
    while(num_test_cases-- > 0) {
        std::cout << prefix_list[num_test_cases%prefix_list.size()] << std::rand() << std::endl;
    }
}

/**
 * @brief evaluate the test cases.
 *
 * @param[in]   pattern         The regex string.
 * @param[in]   file            The test case filename.
 */
void evaluate_test_cases(const std::string& pattern, const std::string& file) {
    hs_database_t*      database;
    hs_compile_error_t* compile_err;
    std::FILE*          fp;
    size_t              data_length;
    int                 fd;
    hs_scratch_t*       scratch = nullptr;
    char*               line;
    void*               test_cases;

    std::cout << "evaluate with pattern:" << pattern << std::endl;

    // 0 - compile pattern
    if (hs_compile(pattern.c_str(), HS_FLAG_DOTALL, HS_MODE_BLOCK, NULL, &database,&compile_err) != HS_SUCCESS) {
        std::cerr << "ERROR: Unabled to compile patter \"" << pattern << "\":"
                  << compile_err->message << std::endl;
        goto error_group_0;
    }
    
    // 1 - get file length
    fp = std::fopen(file.c_str(),"r");
    if (fp ==nullptr) {
        std::cerr << "Failed to open the file. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_1;
    }

    if (std::fseek(fp, 0, SEEK_END) != 0) {
        std::cerr << "Failed to seek to the end of the file. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_2;
    }

    data_length = ftell(fp);
    if (std::fseek(fp, 0, SEEK_SET) != 0) {
        std::cerr << "Failed to seek to the beginning of the file. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_2;
    }

    std::fclose(fp);

    // 2 - mmap file
    fd = open(file.c_str(),O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file:" << file << " for read. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_3;
    }
    test_cases = mmap(nullptr,data_length,PROT_READ,MAP_PRIVATE,fd,0);
    if (test_cases == MAP_FAILED) {
        std::cerr << "Failed to mmap file:" << file << " for read. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_4;
    }

    // 3 - scan to load.
    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        std::cerr << "Failed to allocate scratch space. Error:"
                  << std::strerror(errno) << std::endl;
        goto error_group_5;
    }

    if (hs_scan(database, static_cast<const char*>(test_cases), data_length, 
                0, scratch, nullptr, nullptr) != HS_SUCCESS) {
        std::cerr << "hs_scan() failed during loading...exiting." << std::endl;
        goto error_group_6;
    }

    // 4 - evaluate
    line = reinterpret_cast<char*>(test_cases);
    for (size_t pos = 0;pos < data_length; pos ++) {
        char* p = (reinterpret_cast<char*>(test_cases) + pos);
        if (*p == '\n') {
            derecho::cascade::TimestampLogger::log(5000001,
                    reinterpret_cast<uint64_t>(line),static_cast<uint64_t>(p-line),derecho::cascade::get_time_ns(false));
            if (hs_scan(database, line, p-line, 0, scratch, nullptr,nullptr) != HS_SUCCESS ) {
                std::cerr << "hs_scan() failed during evaluation...exiting." << std::endl;
                goto error_group_5;
            }
            line = p+1;
        }
    }
    std::cout << "done." << std::endl;
    derecho::cascade::TimestampLogger::flush("hs.tt");
    
    // 4 - clean up.
error_group_6:
    hs_free_scratch(scratch);
error_group_5:
    if (munmap(test_cases,data_length) < 0) {
        std::cerr << "Failed to unmap file:" << file << ".Error:"
                 << std::strerror(errno) << std::endl;
    }
error_group_4:
    close(fd);
error_group_3:
    hs_free_database(database);
    return;
error_group_2:
    std::fclose(fp);
error_group_1:
    hs_free_database(database);
error_group_0:
    return;
}

/**
 * @brief The main entry.
 */
int main(int argc, char** argv) {

    // step 0 - parameters
    static struct option long_options[] = {
        {"generate-test-cases",     required_argument,  0,  'g'},
        {"evaluate",                required_argument,  0,  'e'},
        {"pattern",                 required_argument,  0,  'p'},
        {"help",                    no_argument,        0,  'h'},
        {0,0,0,0}
    };

    int c;
    enum {
        OP_NONE,
        OP_GEN,
        OP_EVAL}    op = OP_NONE;
    uint32_t        num_test_cases;
    std::string     testcase_file;
    std::string     pattern;

    while (true) {
        int option_index = 0;
        c = getopt_long(argc,argv,"g:e:p:h",long_options,&option_index);

        if (c == -1) {
            break;
        }

        switch(c) {
        case 'g':
            op = OP_GEN;
            num_test_cases = std::stoul(optarg);
            break;
        case 'e':
            op = OP_EVAL;
            testcase_file = optarg;
            break;
        case 'p':
            pattern = optarg;
            break;
        case 'h':
            std::cout << help_string << std::endl;
            return 0;
        case '?':
        default:
            std::cout << "unknown options." << std::endl;
            std::cout << help_string << std::endl;
            return -1;
        }
    }

    // step 1
    switch (op) {
    case OP_GEN:
        generate_test_cases(num_test_cases);
        break;
    case OP_EVAL:
        evaluate_test_cases(pattern, testcase_file);
        break;
    case OP_NONE:
    default:
        std::cout << help_string << std::endl;
    }
    return 0;
}
