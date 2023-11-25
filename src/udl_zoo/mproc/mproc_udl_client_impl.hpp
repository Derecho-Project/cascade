#pragma once
namespace derecho {
namespace cascade {

template <typename FirstCascadeType,typename ... RestCascadeTypes>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::MProcUDLClient(const key_t object_commit_rb) {
    //TODO:
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
void MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::submit(
    const node_id_t             sender,
    const std::string&          object_pool_pathname,
    const std::string&          key_string,
    const ObjectWithStringKey&  object,
    uint32_t                    worker_id) {
    // TODO
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::~MProcUDLClient() {
    //TODO
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
std::unique_ptr<MProcUDLClient<FirstCascadeType,RestCascadeTypes...>>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::create() {
    //TODO
    return nullptr;
}

}
}
