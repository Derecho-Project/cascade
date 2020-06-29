namespace derecho{
namespace cascade{


template <typename... CascadeTypes>
Service<CascadeTypes...>::Service(const json& layout) {
    // TODO:
    // STEP 1 - load configuration
    // STEP 2 - create derecho group
    // STEP 3 - create service thread
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::run() {
    // TODO:
    std::lock_guard<std::mutex> lck(this->service_control_mutex);
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::stop() {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->_is_running = false;
    lck.unlock();
    this->service_control_cv.notify_one();
    // wait until stopped.
    if (this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
bool Service<CascadeTypes...>::is_running() {
    std::lock_guard<std::mutex> lck(this->service_control_mutex);
    return _is_running;
}

template <typename... CascadeTypes>
std::unique_ptr<Service<CascadeTypes...>> service_ptr;

template <typename... CascadeTypes>
void Service<CascadeTypes...>::start() {
    if (service_ptr) {
        service_ptr = std::make_unique<Service<CascadeTypes...>>();
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::shutdown() {
    if (service_ptr) {
        if (service_ptr->is_running()) {
            service_ptr->stop();
        }
    }
}

}
}
