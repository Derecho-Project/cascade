#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <iostream>

namespace derecho {
namespace cascade {

/**
 * @tparam T            - the value type
 * @tparam separator    - the prefix separator
 */
template <typename T, char separator = '/'>
class PrefixRegistry {
private:
    class TreeNode {
    public:
        const std::string component;
        std::shared_ptr<T> value;
        std::unordered_map<std::string,std::unique_ptr<TreeNode>> children;
        TreeNode();
        TreeNode(const std::string& comp,std::shared_ptr<T> shared_ptr_value=nullptr);
        virtual ~TreeNode();
        void dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer ) const;
    };
    TreeNode prefix_tree;
    mutable std::mutex prefix_tree_mutex;

    /**
     * Get the tree node from components. Require lock to be acquired in advance.
     *
     * @param components - the components of the prefix.
     *
     * @return a const pointer to the tree node, or nullptr if tree node is not found.
     */
    inline TreeNode* get_tree_node(const std::vector<std::string>& components);
    inline const TreeNode* get_tree_node_const(const std::vector<std::string>& components) const;
    /**
     * Register a prefix using its components
     *
     * @param components - the components of the prefix.
     * @param value - the value to be regsitered, which is passed by value.
     *
     * @return true for successful registration, false if the prefix has already been registered.
     */
    inline bool _register_prefix(const std::vector<std::string>& components, const T& value);
public:
    /**
     * Register a prefix.
     * The prefix has to be in this format: "/component1/component2/.../componentn/". Please note that any characters after
     * the trailing separator '/' is ignored.
     *
     * @param prefix - the prefix to be registered
     * @param value - the value to be registered
     *
     * @return true if the prefix is successfully registered, false if a prefix is already registered.
     */
    bool register_prefix(const std::string& prefix, const T& value);

    /**
     * Remove a prefix
     * @param prefix - the prefix to be removed
     *
     * @return true if the prefix is successfully removed, false if the prefix is not found.
     */
    bool remove_prefix(const std::string& prefix);
    /**
     * Atomically modify an entry
     *
     * @param prefix    - the prefix to be registered
     * @param modifier  - the modifier lambda
     * @param create    - if true, create all the absent tree nodes in the prefix path; otherwise, no action is taken.
     */
    void atomically_modify(const std::string& prefix, const std::function<std::shared_ptr<T>(const std::shared_ptr<T>& value)>& modifier, bool create = false);
    /**
     * Test if a prefix has already been registered or not.
     *
     * @param prefix - the prefix
     *
     * @return true if the prefix is registered, false otherwise.
     */
    bool is_registered(const std::string& prefix);
    /**
     * Get a reference to the value corresponding to the prefix, exception will be thrown if the prefix is not
     * registered.
     *
     * @param prefix - the prefix
     *
     * @return a shared pointer to the value for the prefix
     */
    std::shared_ptr<T> get_value(const std::string& prefix) const;
    /**
     * Process the values for all registered prefixes
     * A path has to be in this format: "/component1/component2/.../componentn/filename".
     *
     * @param path      - the full path.
     * @param collector - the lambda function to collect values for all prefixes of a string.
     *
     * @return 
     */
    void collect_values_for_prefixes(const std::string& path,
            const std::function<void(const std::string& prefix,const std::shared_ptr<T>& value)>& collector) const;
#ifdef PREFIX_REGISTRY_DEBUG
    /**
     * Dump the tree information
     * @param out       - the output stream
     */
    void dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer) const;
    void dump(std::ostream& out, const std::function<void(std::ostream&,const T&)>& value_printer, const TreeNode* ptn, uint32_t indention) const;
    /**
     * Pick a random prefix
     */
    std::string pick_random_prefix() const;
#endif
    /**
     * constructor
     */
    PrefixRegistry();
    /**
     * destructor
     */
    virtual ~PrefixRegistry();
};

} // namespace cascade
} // namespace derecho

#include "prefix_registry_impl.hpp"
