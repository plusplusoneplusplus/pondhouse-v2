#pragma once

#include <atomic>
#include <cassert>
#include <memory>
#include <random>

#include "common/data_chunk.h"
#include "common/iterator.h"

namespace pond::common {

template <typename K, typename V>
class SkipList {
private:
    struct Node;
    using NodePtr = std::atomic<Node*>;

    struct Node {
        const K key;
        V value;
        const int height;
        std::unique_ptr<NodePtr[]> next;

        Node(const K& k, const V&& v, int h) : key(k), value(std::move(v)), height(h), next(new NodePtr[h]) {
            for (int i = 0; i < h; i++) {
                next[i] = nullptr;
            }
        }

        ~Node() = default;
    };

    Node* head_;
    std::atomic<int> max_height_;
    const int max_level_;
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_real_distribution<> dis_;
    std::atomic<size_t> size_{0};  // Track number of elements

    int getRandomHeight() {
        int height = 1;
        while (height < max_level_ && dis_(gen_) < 0.25) {
            height++;
        }
        return height;
    }

    Node* findGreaterOrEqual(const K& key, Node** prev) const {
        Node* x = head_;
        int level = max_height_.load(std::memory_order_relaxed) - 1;

        while (true) {
            Node* next = x->next[level].load(std::memory_order_acquire);
            if (next && next->key < key) {
                x = next;
            } else {
                if (prev) {
                    prev[level] = x;
                }
                if (level == 0) {
                    return next;
                }
                level--;
            }
        }
    }

public:
    explicit SkipList(int max_level = 12) : max_level_(max_level), gen_(rd_()), dis_(0, 1) {
        head_ = new Node(K(), V(), max_level_);
        max_height_.store(1, std::memory_order_relaxed);
        size_.store(0, std::memory_order_relaxed);
    }

    ~SkipList() {
        Node* current = head_;
        while (current != nullptr) {
            Node* next = current->next[0].load(std::memory_order_relaxed);
            delete current;
            current = next;
        }
    }

    void Insert(const K& key, const V&& value) {
        Node* prev[max_level_];
        Node* x = findGreaterOrEqual(key, prev);

        // Don't allow duplicate insertion
        if (x && x->key == key) {
            x->value = std::move(value);
            return;
        }

        int height = getRandomHeight();
        int max_height = max_height_.load(std::memory_order_relaxed);

        if (height > max_height) {
            for (int i = max_height; i < height; i++) {
                prev[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = new Node(key, std::move(value), height);
        for (int i = 0; i < height; i++) {
            x->next[i].store(prev[i]->next[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
            prev[i]->next[i].store(x, std::memory_order_release);
        }
        size_.fetch_add(1, std::memory_order_relaxed);
    }

    bool Contains(const K& key) const {
        Node* x = findGreaterOrEqual(key, nullptr);
        return (x != nullptr && x->key == key);
    }

    bool Get(const K& key, V& value) const {
        Node* x = findGreaterOrEqual(key, nullptr);
        if (x && x->key == key) {
            value = x->value;
            return true;
        }
        return false;
    }

    /**
     * Returns the number of elements in the skip list.
     * @return The number of elements in the skip list.
     */
    [[nodiscard]] size_t Size() const { return size_.load(std::memory_order_relaxed); }

    class SkipListIterator : public Iterator<K, V> {
    private:
        const SkipList* list_;
        Node* node_;

    public:
        explicit SkipListIterator(const SkipList* list)
            : list_(list), node_(list->head_->next[0].load(std::memory_order_acquire)) {}

        bool Valid() const override { return node_ != nullptr; }
        const K& key() const override {
            if (!Valid()) {
                throw std::runtime_error("Iterator is not valid");
            }

            return node_->key;
        }
        const V& value() const override {
            if (!Valid()) {
                throw std::runtime_error("Iterator is not valid");
            }

            return node_->value;
        }

        void Next() override {
            if (!Valid()) {
                throw std::runtime_error("Iterator is not valid");
            }

            node_ = node_->next[0].load(std::memory_order_acquire);
        }

        void Seek(const K& target) override { node_ = list_->findGreaterOrEqual(target, nullptr); }

        bool IsTombstone() const override { return false; }
    };

    Iterator<K, V>* NewIterator() const { return new SkipListIterator(this); }
};

}  // namespace pond::common
