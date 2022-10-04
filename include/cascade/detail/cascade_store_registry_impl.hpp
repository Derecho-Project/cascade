
namespace derecho {
namespace cascade {

template <typename SubgroupType>
void CascadeStoreRegistry::register_cascade_store(const SubgroupType *instance){
    cascade_store[std::type_index(typeid(SubgroupType))] = (const void*)instance;
}

template <typename SubgroupType>
const SubgroupType* CascadeStoreRegistry::get_cascade_store(){
    auto ti = std::type_index(typeid(SubgroupType));
    if(cascade_store.count(ti) > 0){
        return (const SubgroupType*)cascade_store[ti];
    }
    return nullptr;
}

}
}
