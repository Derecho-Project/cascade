#pragma once

#include <memory>
#include <string>
#include <spdlog/spdlog.h>

namespace wan_agent {

#ifdef NDEBUG
    #define log_trace(...)
    #define log_debug(...)
    #define log_info(...)
    #define log_warn(...)
    #define log_error(...)
    #define log_crit(...)
    #define log_flush()
    #define log_enter_func()
    #define log_exit_func()
#else//NDEBUG

class Logger {
private:
    static std::shared_ptr<spdlog::logger> _default_logger;
    static std::mutex initialization_mutex;

public:
    static spdlog::logger* getDefaultLogger();
};

    #define log_trace(...)  Logger::getDefaultLogger()->trace(__VA_ARGS__)
    #define log_debug(...)  Logger::getDefaultLogger()->debug(__VA_ARGS__)
    #define log_info(...)   Logger::getDefaultLogger()->info(__VA_ARGS__)
    #define log_warn(...)   Logger::getDefaultLogger()->warn(__VA_ARGS__)
    #define log_error(...)  Logger::getDefaultLogger()->error(__VA_ARGS__)
    #define log_crit(...)   Logger::getDefaultLogger()->crit(__VA_ARGS__)
    #define log_flush()     Logger::getDefaultLogger()->flush()
    #define log_enter_func()    log_trace("Entering {}.", __func__)
    #define log_exit_func()     log_trace("Exiting {}.", __func__)
#endif//NDEBUG

}