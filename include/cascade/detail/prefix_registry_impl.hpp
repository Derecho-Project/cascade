#pragma once
#include "cascade/utils.hpp"

#include <stdexcept>
#include <cstring>

namespace derecho {
namespace cascade {

template <typename T, char separator>
PrefixRegistry<T,separator>::TreeNode::TreeNode() {}

template <typename T, char separator>
PrefixRegistry<T,separator>::TreeNode::TreeNode(const std::string& comp, std::shared_ptr<T> shared_ptr_value):
    component(comp) {
    value = shared_ptr_value;
}

template <typename T, char separator>
PrefixRegistry<T,separator>::TreeNode::~TreeNode() {}

template <typename T, char separator>
PrefixRegistry<T,separator>::PrefixRegistry() {}

template <typename T, char separator>
PrefixRegistry<T,separator>::~PrefixRegistry() {}

template <typename T, char separator>
const typename PrefixRegistry<T,separator>::TreeNode* PrefixRegistry<T,separator>::get_tree_node_const(const std::vector<std::string>& components) const {
    const TreeNode* ptn = &prefix_tree;
    for (const auto& comp:components) {
        if(ptn->children.find(comp) == ptn->children.end()) {
            ptn = nullptr;
            break;
        }
        ptn = ptn->children.at(comp).get();
    }
    return ptn;
}

template <typename T, char separator>
typename PrefixRegistry<T,separator>::TreeNode* PrefixRegistry<T,separator>::get_tree_node(const std::vector<std::string>& components) {
    TreeNode* ptn = &prefix_tree;
    for (const auto& comp:components) {
        if(ptn->children.find(comp) == ptn->children.end()) {
            ptn = nullptr;
            break;
        }
        ptn = ptn->children.at(comp).get();
    }
    return ptn;
}


template <typename T, char separator>
bool PrefixRegistry<T, separator>::_register_prefix(const std::vector<std::string>& components, const T& value) {
    std::lock_guard<std::mutex> lck(prefix_tree_mutex);

    TreeNode* ptn = &prefix_tree;
    for (const auto& comp:components) {
        if (ptn->children.find(comp) == ptn->children.end()) {
            ptn->children.emplace(comp,std::make_unique<TreeNode>(comp));
        }
        ptn = ptn->children.at(comp).get();
    }

    if (ptn->value) {
        // already set.
        return false;
    } else {
        ptn->value = std::make_shared<T>(value);
    }

    return true;
}

template <typename T, char separator>
bool PrefixRegistry<T,separator>::register_prefix(const std::string &prefix, const T &value) {
    return _register_prefix(str_tokenizer(prefix,true,separator),value);
}

template <typename T, char separator>
bool PrefixRegistry<T, separator>::remove_prefix(const std::string& prefix) {
    std::lock_guard<std::mutex> lck(prefix_tree_mutex);
    auto ptn = get_tree_node(str_tokenizer(prefix,true,separator));
    if (ptn == nullptr || !ptn->value) {
        return false;
    }
    ptn->value.reset();
    return true;
}

template <typename T, char separator>
void PrefixRegistry<T, separator>::atomically_modify(const std::string& prefix, const std::function<std::shared_ptr<T>(const std::shared_ptr<T>& value)>& modifier, bool create) {

    auto components = str_tokenizer(prefix,true,separator);
    std::lock_guard<std::mutex> lck(prefix_tree_mutex);

    TreeNode* ptn = &prefix_tree;
    for (const auto& comp:components) {
        if (ptn->children.find(comp) == ptn->children.end()) {
            if (create) {
                ptn->children.emplace(comp,std::make_unique<TreeNode>(comp));
            } else {
                // skip absent prefix.
                return;
            }
        }
        ptn = ptn->children.at(comp).get();
    }

    ptn->value = modifier(ptn->value);
}

template <typename T, char separator>
bool PrefixRegistry<T, separator>::is_registered(const std::string& prefix) {
    const auto ptr = get_tree_node_const(str_tokenizer(prefix,true,separator));
    return ((ptr!=nullptr) && ptr->value);
}

template <typename T, char separator>
void PrefixRegistry<T, separator>::TreeNode::dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer) const {
    out << "{comp:"<<component<<"; value:";
    if (value) {
        value_printer(out,*value);
    } else {
        out << "nullptr";
    }
    out <<"; children:" << children.size() << ";}";
}

#ifdef PREFIX_REGISTRY_DEBUG
template <typename T, char separator>
void PrefixRegistry<T, separator>::dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer) const {
    dump(out,value_printer,&prefix_tree,0);
}

template <typename T, char separator>
void PrefixRegistry<T, separator>::dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer, const TreeNode* ptn, uint32_t indention) const {
    uint32_t ind = indention;
    while(ind--) {
        out << "    ";
    }
    ptn->dump(out,value_printer);
    out << "\n";
    for (const auto& kv: ptn->children) {
        dump(out,value_printer,kv.second.get(),indention+1);
    }
}

template <typename T, char separator>
std::string PrefixRegistry<T, separator>::pick_random_prefix() const {
    std::string prefix(1,separator);

    const TreeNode* ptn = &prefix_tree;

    while (ptn->children.size()) {
        const auto random_it = std::next(std::begin(ptn->children), rand()%ptn->children.size());
        prefix = prefix + random_it->second->component + separator;
        ptn = random_it->second.get();
    }

    return prefix;
}
#endif

template <typename T, char separator>
std::shared_ptr<T> PrefixRegistry<T, separator>::get_value(const std::string& prefix) const {
    std::lock_guard<std::mutex> lck(prefix_tree_mutex);
    const auto ptn = get_tree_node_const(str_tokenizer(prefix,true,separator));
    if (ptn) {
        return ptn->value;
    }
    return nullptr;
}

template <typename T, char separator>
void PrefixRegistry<T, separator>::collect_values_for_prefixes(
        const std::string& path,
        const std::function<void(const std::string& prefix,const std::shared_ptr<T>& value)>& collector) const {
    auto components = str_tokenizer(path,true,separator);
    const TreeNode* ptn = &prefix_tree;
    std::lock_guard<std::mutex> lck(prefix_tree_mutex);
    std::string prefix(1,separator);
    for (const auto& comp:components) {
        prefix = prefix + comp + separator;
        if (ptn->children.find(comp) == ptn->children.end()) {
            break;
        }
        ptn = ptn->children.at(comp).get();
        if (ptn->value) {
            collector(prefix,ptn->value);
        }
    }
}

} // namespace cascade
} // namespace derecho
