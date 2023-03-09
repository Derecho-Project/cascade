#include <filesystem>
#include <vector>
namespace fs = std::filesystem;

template <typename T>
struct PathTree {
    std::string label;
    T data;

    PathTree<T>* parent;
    std::unordered_map<std::string, PathTree<T>*> children;

    PathTree(std::string label, T data, PathTree<T>* parent)
            : label(label), data(data), parent(parent) {}
    PathTree(std::string label, T data) : PathTree(label, data, nullptr) {}

    ~PathTree() {
        for(auto& [k, v] : children) {
            delete v;
        }
    }

    std::vector<std::string> entries() const {
        std::vector<std::string> res;
        res.reserve(children.size());
        for(const auto& [k, _] : children) {
            res.push_back(k);
        }
        return res;
    }

    std::string absolute_path() const {
        std::vector<std::string> parts;
        for(const PathTree<T>* node = this; node != nullptr; node = node->parent) {
            parts.push_back(node->label);
        }
        std::string res;
        for(auto it = parts.rbegin(); it != parts.rend(); ++it) {
            if(res.compare("") != 0 && res.compare("/") != 0) {
                res += "/";
            }
            res += *it;
        }

        return res;
    }

    void print(int depth = 0, std::ostream& stream = std::cout,
               int pad = 0) const {
        for(int i = 0; i < pad - 1; ++i) {
            stream << "  ";
        }
        if(pad) {
            stream << "|-";
        }
        stream << label << "\n";
        if(children.empty() || depth == 0) {
            return;
        }
        for(const auto& [_, v] : children) {
            v->print(depth - 1, stream, pad + 1);
        }
        if(pad == 0) {
            stream << "\n\n";
        }
    }

    // returns nullptr on fail or if location already exists
    PathTree<T>* set(const fs::path& path, T intermediate, T data) {
        if(path.empty()) {
            return nullptr;
        }
        auto it = path.begin();
        if(*it != label) {
            return nullptr;
        }
        bool created_new = false;
        PathTree<T>* cur = this;
        for(++it; it != path.end(); ++it) {
            if(!cur->children.count(*it)) {
                created_new = true;
                PathTree<T>* next = new PathTree<T>(*it, intermediate, cur);
                cur->children.insert({*it, next});
                cur = next;
            } else {
                cur = cur->children.at(*it);
            }
        }
        if(!created_new) {
            return nullptr;
        }
        cur->data = data;
        return cur;
    }

    // returns nullptr on fail or location does not exist
    PathTree<T>* get(const fs::path& path) {
        if(path.empty()) {
            return nullptr;
        }
        auto it = path.begin();
        if(*it != label) {
            return nullptr;
        }
        PathTree<T>* cur = this;
        for(++it; it != path.end(); ++it) {
            if(!cur->children.count(*it)) {
                return nullptr;
            }
            cur = cur->children.at(*it);
        }
        return cur;
    }

    PathTree<T>* get_while_valid(const fs::path& path) {
        if(path.empty()) {
            return nullptr;
        }
        auto it = path.begin();
        if(*it != label) {
            return nullptr;
        }
        PathTree<T>* cur = this;
        for(++it; it != path.end(); ++it) {
            if(!cur->children.count(*it)) {
                return cur;
            }
            cur = cur->children.at(*it);
        }
        return cur;
    }

    PathTree<T>* extract(const fs::path& path) {
        // TODO use??
        PathTree<T>* cur = get(path);
        if(cur == nullptr || cur->parent == nullptr) {
            return nullptr;
        }
        PathTree<T>* par = cur->parent;
        par->children.erase(cur->label);

        cur->parent = nullptr;
        return cur;
    }

    // deletes node and replaces with replacement. updating parent
    // requires same label
    static bool replace(PathTree<T>* node, PathTree<T>* replacement) {
        if(node == nullptr || replacement == nullptr || node->label != replacement->label || node->parent == nullptr) {
            return false;
        }
        std::cout << "replacing " << node->absolute_path() << std::endl;
        PathTree<T>* par = node->parent;
        replacement->parent = par;
        par->children[node->label] = replacement;
        delete node;
        return true;
    }
};
