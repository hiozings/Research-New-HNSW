#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <fstream>
#include <list>
#include <utility>
#include "../httplib.h"

struct NodeOffset {
    uint64_t offset;
    uint32_t degree;
};

// 简单的LRU缓存
template<typename K, typename V>
class LRUCache {
private:
    size_t capacity_;
    std::list<std::pair<K, V>> cache_list_;
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> cache_map_;

public:
    LRUCache(size_t capacity) : capacity_(capacity) {}
    
    V* get(const K& key) {
        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) return nullptr;
        
        // 移动到前面
        cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
        return &(it->second->second);
    }
    
    void put(const K& key, const V& value) {
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // 更新现有值并移动到前面
            it->second->second = value;
            cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
            return;
        }
        
        // 添加新元素
        cache_list_.emplace_front(key, value);
        cache_map_[key] = cache_list_.begin();
        
        // 如果超过容量，移除最旧的
        if (cache_map_.size() > capacity_) {
            auto last = cache_list_.end();
            last--;
            cache_map_.erase(last->first);
            cache_list_.pop_back();
        }
    }
};

struct HNSWGraph {
    std::vector<std::vector<uint32_t>> adjacency;
    std::unordered_map<uint32_t, size_t> id_to_index;
    // std::unordered_map<uint32_t, NodeOffset> offset_map;
    std::vector<std::unordered_map<uint32_t, NodeOffset>> level_offsets;
    std::vector<std::unordered_map<uint32_t, std::vector<uint32_t>>> levels_neighbors;
    
    bool optimized = false;
    std::string graph_file_path;
    uint32_t entrypoint = 0;
    size_t max_level = 0;

    // 缓存
    mutable LRUCache<uint32_t, std::vector<uint32_t>> neighbors_cache{10000};
    mutable std::unique_ptr<std::ifstream> file_stream;
    mutable std::unique_ptr<httplib::Client> http_client;
    mutable std::unordered_map<uint32_t, std::vector<float>> vector_cache;// 简单缓存已获取的向量


    bool load_from_file(const std::string& path, bool optimized = false);
    void initialize_http_client(const std::string& storage_url) const;

    // HNSW搜索函数
    std::vector<std::pair<uint32_t, float>> search_candidates(
        const HNSWGraph& g, const std::string& storage_url, 
        const std::vector<float>& query, uint32_t entry_id, 
        size_t ef, size_t k) const;

    // 分层搜索
    uint32_t search_layer_original(const std::string& storage_url,
                                  const std::vector<float>& query,
                                  uint32_t entry_point, 
                                  int level, size_t ef) const;
    
    std::vector<std::pair<uint32_t, float>> search_base_layer_original(
        const std::string& storage_url,
        const std::vector<float>& query,
        uint32_t entry_point, size_t ef, size_t k) const;

    // 工具函数
    float l2_sq(const std::vector<float>& a, const std::vector<float>& b) const;
    std::vector<float> fetch_vector(const std::string& storage_url, uint32_t id) const;
    // std::vector<uint32_t> load_neighbors(uint32_t id) const;
    std::vector<uint32_t> get_neighbors(uint32_t id, int level = 0) const;
};