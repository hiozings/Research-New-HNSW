#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>

// struct NodeOffset {
//     uint64_t offset;
//     uint32_t degree;
// };

// struct HNSWGraph {
//     std::vector<std::vector<uint32_t>> adjacency;        // 邻接表表示图（普通）
//     std::unordered_map<uint32_t, size_t> id_to_index;    // 结点id到内部索引的映射
//     std::unordered_map<uint32_t, NodeOffset> offset_map; // 结点id到文件偏移的映射（优化）
//     bool optimized = false; // 是否使用优化
//     std::string graph_file_path; // 图文件路径（优化）
//     uint32_t entrypoint = 0;
//     size_t max_level = 0;

//     bool load_from_file(const std::string& path, bool optimized = false);

//     std::vector<std::pair<uint32_t,float>> search_candidates(const HNSWGraph& g, const std::string& storage_url, const std::vector<float>& query,
//     uint32_t entry_id, size_t ef, size_t k) const;
//     float l2_sq(const std::vector<float>& a, const std::vector<float>& b) const;
//     std::vector<float> fetch_vector(const std::string& storage_url, uint32_t id) const;
//     std::vector<uint32_t> load_neighbors(uint32_t id) const;
// };

#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <memory>
#include <fstream>

struct NodeOffset {
    uint64_t offset;
    uint32_t degree;
};

struct HNSWGraph {
    std::vector<std::vector<uint32_t>> adjacency;        // 邻接表表示图（普通）
    std::unordered_map<uint32_t, size_t> id_to_index;    // 结点id到内部索引的映射
    std::unordered_map<uint32_t, NodeOffset> offset_map; // 结点id到文件偏移的映射（优化）
    std::vector<std::unordered_map<uint32_t, std::vector<uint32_t>>> levels_neighbors; // 分层邻居信息
    
    bool optimized = false;
    std::string graph_file_path;
    uint32_t entrypoint = 0;
    size_t max_level = 0;

    // 资源管理
    mutable std::unique_ptr<std::ifstream> file_stream;
    mutable std::unique_ptr<httplib::Client> http_client;

    bool load_from_file(const std::string& path, bool optimized = false);
    void initialize_http_client(const std::string& storage_url) const;

    // 完整的HNSW搜索
    std::vector<std::pair<uint32_t, float>> search_candidates(
        const HNSWGraph& g, const std::string& storage_url, 
        const std::vector<float>& query, uint32_t entry_id, 
        size_t ef, size_t k) const;

    // 分层搜索辅助函数
    uint32_t search_layer(const std::string& storage_url, 
                         const std::vector<float>& query,
                         uint32_t entry_point, 
                         int level, size_t ef) const;
    
    std::vector<std::pair<uint32_t, float>> search_base_layer(
        const std::string& storage_url,
        const std::vector<float>& query,
        uint32_t entry_point, size_t ef, size_t k) const;

    // 工具函数
    float l2_sq(const std::vector<float>& a, const std::vector<float>& b) const;
    std::vector<float> fetch_vector(const std::string& storage_url, uint32_t id) const;
    std::vector<uint32_t> load_neighbors(uint32_t id) const;
    std::vector<uint32_t> get_neighbors(uint32_t id, int level = 0) const;
    
private:
    std::vector<std::pair<uint32_t, float>> select_neighbors(
        const std::string& storage_url,
        const std::vector<float>& query,
        const std::vector<std::pair<uint32_t, float>>& candidates, 
        size_t M) const;
};
