#include <spdlog/sinks/stdout_color_sinks.h>
#include <wan_agent/logger.hpp>

namespace wan_agent {
#ifndef NDEBUG
std::shared_ptr<spdlog::logger> Logger::_default_logger;
std::mutex Logger::initialization_mutex;
std::string Logger::log_level;

spdlog::logger* Logger::getDefaultLogger() {
    std::unique_lock lock(initialization_mutex);
    if(_default_logger == nullptr) {
        spdlog::stdout_color_mt("wan_agent");
        _default_logger = spdlog::get("wan_agent");
        _default_logger->set_level(spdlog::level::from_str(Logger::log_level));
    }
    lock.unlock();
    return _default_logger.get();
}

void Logger::set_log_level(std::string _log_level) {
    log_level = _log_level;
}
#endif  //NDEBUG
}  // namespace wan_agent
