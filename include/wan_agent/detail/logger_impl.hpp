#include <spdlog/sinks/stdout_color_sinks.h>

namespace wan_agent {
#ifndef NDEBUG
std::shared_ptr<spdlog::logger> Logger::_default_logger;
std::mutex Logger::initialization_mutex;

spdlog::logger* Logger::getDefaultLogger() {
    std::unique_lock lock(initialization_mutex);
    if (_default_logger == nullptr) {
        spdlog::stdout_color_mt("wan_agent");
        _default_logger = spdlog::get("wan_agent");
        _default_logger->set_level(spdlog::level::trace);
    }
    lock.unlock();
    return _default_logger.get();
}
#endif//NDEBUG
}
