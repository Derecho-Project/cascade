
namespace derecho {
namespace cascade {

template <typename SubgroupType>
void CascadeStoreRegistry::register_cascade_store(SubgroupType *instance){
    cascade_store[std::type_index(typeid(SubgroupType))] = (void*)instance;
}

template <typename SubgroupType>
SubgroupType* CascadeStoreRegistry::get_cascade_store(){
    auto ti = std::type_index(typeid(SubgroupType));
    if(cascade_store.count(ti) > 0){
        return (SubgroupType*)cascade_store[ti];
    }
    return nullptr;
}

}
}
