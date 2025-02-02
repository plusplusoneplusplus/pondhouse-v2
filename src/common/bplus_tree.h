#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "common/result.h"

namespace pond::common {

template <typename K, typename V>
class BPlusTree {
public:
    static constexpr size_t MIN_KEYS_PER_NODE = 4;  // Minimum number of keys per node (except root)
    static constexpr size_t MAX_KEYS_PER_NODE = 2 * MIN_KEYS_PER_NODE;  // Maximum number of keys per node

    struct Node {
        bool is_leaf;
        std::vector<K> keys;
        std::vector<V> values;      // For leaf nodes
        std::vector<Node*> children;  // For internal nodes
        Node* next;                   // For leaf nodes (range queries)

        explicit Node(bool leaf = false) : is_leaf(leaf), next(nullptr) {}
        ~Node() {
            if (!is_leaf) {
                for (auto* child : children) {
                    delete child;
                }
            }
        }

        // Prevent copying
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;
    };

    BPlusTree() : root_(nullptr) {}
    ~BPlusTree() { delete root_; }

    // Prevent copying
    BPlusTree(const BPlusTree&) = delete;
    BPlusTree& operator=(const BPlusTree&) = delete;

    // Core operations
    Result<void> Insert(const K& key, const V& value);
    Result<std::optional<V>> Get(const K& key) const;
    Result<std::vector<std::pair<K, V>>> Range(const K& start_key, const K& end_key, size_t limit) const;

    // Utility functions
    bool Empty() const { return root_ == nullptr; }
    void Clear() {
        delete root_;
        root_ = nullptr;
    }

private:
    // Internal helper functions
    Result<Node*> FindLeaf(const K& key) const;
    Result<void> SplitChild(Node* parent, size_t child_idx);
    Result<void> InsertNonFull(Node* node, const K& key, const V& value);
    
    // The root node of the tree
    Node* root_;
};

// Template implementation
template <typename K, typename V>
Result<void> BPlusTree<K, V>::Insert(const K& key, const V& value) {
    if (root_ == nullptr) {
        root_ = new Node(true);  // Create a leaf node as root
    }

    // If root is full, split it
    if (root_->keys.size() == MAX_KEYS_PER_NODE) {
        Node* new_root = new Node(false);
        new_root->children.push_back(root_);
        root_ = new_root;
        RETURN_IF_ERROR(SplitChild(new_root, 0));
    }

    return InsertNonFull(root_, key, value);
}

template <typename K, typename V>
Result<std::optional<V>> BPlusTree<K, V>::Get(const K& key) const {
    if (root_ == nullptr) {
        return Result<std::optional<V>>::success(std::nullopt);
    }

    auto leaf_result = FindLeaf(key);
    if (!leaf_result.ok()) {
        return Result<std::optional<V>>::failure(leaf_result.error());
    }

    Node* leaf = leaf_result.value();
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it != leaf->keys.end() && *it == key) {
        size_t idx = it - leaf->keys.begin();
        return Result<std::optional<V>>::success(leaf->values[idx]);
    }

    return Result<std::optional<V>>::success(std::nullopt);
}

template <typename K, typename V>
Result<std::vector<std::pair<K, V>>> BPlusTree<K, V>::Range(
    const K& start_key, const K& end_key, size_t limit) const {
    std::vector<std::pair<K, V>> result;
    if (root_ == nullptr || limit == 0 || start_key > end_key) {
        return Result<std::vector<std::pair<K, V>>>::success(result);
    }

    // Find the leaf node containing start_key
    auto leaf_result = FindLeaf(start_key);
    if (!leaf_result.ok()) {
        return Result<std::vector<std::pair<K, V>>>::failure(leaf_result.error());
    }

    Node* leaf = leaf_result.value();
    if (leaf == nullptr) {
        return Result<std::vector<std::pair<K, V>>>::success(result);
    }

    // Traverse leaf nodes and collect entries
    while (leaf != nullptr && result.size() < limit) {
        auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), start_key);
        for (; it != leaf->keys.end() && result.size() < limit; ++it) {
            size_t idx = it - leaf->keys.begin();
            if (*it > end_key) {
                return Result<std::vector<std::pair<K, V>>>::success(result);
            }
            result.emplace_back(*it, leaf->values[idx]);
        }
        leaf = leaf->next;
    }

    return Result<std::vector<std::pair<K, V>>>::success(result);
}

template <typename K, typename V>
Result<typename BPlusTree<K, V>::Node*> BPlusTree<K, V>::FindLeaf(const K& key) const {
    Node* current = root_;
    while (current != nullptr && !current->is_leaf) {
        auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
        size_t idx = it - current->keys.begin();
        current = current->children[idx];
    }
    return Result<Node*>::success(current);
}

template <typename K, typename V>
Result<void> BPlusTree<K, V>::SplitChild(Node* parent, size_t child_idx) {
    Node* child = parent->children[child_idx];
    Node* new_node = new Node(child->is_leaf);

    // Move half of the keys and values to the new node
    size_t mid = child->keys.size() / 2;

    if (child->is_leaf) {
        // For leaf nodes, keep all keys (including the middle key)
        new_node->keys.assign(child->keys.begin() + mid, child->keys.end());
        new_node->values.assign(child->values.begin() + mid, child->values.end());
        child->keys.resize(mid);
        child->values.resize(mid);
        
        // Update leaf node links
        new_node->next = child->next;
        child->next = new_node;
        
        // Use the first key of new node as separator
        K separator_key = new_node->keys.front();
        
        // Insert separator key and new node pointer into parent
        auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), separator_key);
        size_t insert_idx = it - parent->keys.begin();
        parent->keys.insert(parent->keys.begin() + insert_idx, separator_key);
        parent->children.insert(parent->children.begin() + insert_idx + 1, new_node);
    } else {
        // For internal nodes, move keys after the middle key
        new_node->keys.assign(child->keys.begin() + mid + 1, child->keys.end());
        new_node->children.assign(child->children.begin() + mid + 1, child->children.end());
        
        // Get the middle key as separator (it goes to parent)
        K separator_key = child->keys[mid];
        
        // Resize child node (exclude the middle key)
        child->keys.resize(mid);
        child->children.resize(mid + 1);
        
        // Insert separator key and new node pointer into parent
        auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), separator_key);
        size_t insert_idx = it - parent->keys.begin();
        parent->keys.insert(parent->keys.begin() + insert_idx, separator_key);
        parent->children.insert(parent->children.begin() + insert_idx + 1, new_node);
    }

    return Result<void>::success();
}

template <typename K, typename V>
Result<void> BPlusTree<K, V>::InsertNonFull(Node* node, const K& key, const V& value) {
    if (node->is_leaf) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        size_t idx = it - node->keys.begin();
        
        if (it != node->keys.end() && *it == key) {
            // Update existing key
            node->values[idx] = value;
        } else {
            // Insert new key-value pair
            node->keys.insert(node->keys.begin() + idx, key);
            node->values.insert(node->values.begin() + idx, value);
        }
        return Result<void>::success();
    }

    // Find the child to recurse into
    auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
    size_t idx = it - node->keys.begin();
    
    // If we found an exact match, use the next child
    if (it != node->keys.end() && *it == key) {
        idx++;
    }
    
    Node* child = node->children[idx];

    // Split child if full
    if (child->keys.size() == MAX_KEYS_PER_NODE) {
        RETURN_IF_ERROR(SplitChild(node, idx));
        // After splitting, determine which child to follow
        it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        idx = it - node->keys.begin();
        if (it != node->keys.end() && key >= *it) {
            idx++;
        }
        child = node->children[idx];
    }

    return InsertNonFull(child, key, value);
}

}  // namespace pond::common
