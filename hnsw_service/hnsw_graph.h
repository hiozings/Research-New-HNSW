#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>

// struct HNSWNode {
// uint32_t id;
// std::vector<std::vector<uint32_t>> levels; // 分层邻居列表
// };

// class HNSWGraph 
// {
//     public:
//         bool load_from_file(const std::string& path);
//         std::vector<uint32_t> search_candidates(uint32_t entrypoint, size_t ef);
//         uint32_t entrypoint() const { return entrypoint_id_; } // 获取入口点
//         size_t max_level() const { return max_level_; } // 获取最大层数

//     private:
//         std::vector<HNSWNode> nodes_;
//         uint32_t entrypoint_id_ = 0;
//         size_t max_level_ = 0;
// };

struct NodeOffset {
    uint64_t offset;
    uint32_t degree;
};

struct HNSWGraph {
    std::vector<std::vector<uint32_t>> adjacency;        // 邻接表表示图（普通）
    std::unordered_map<uint32_t, size_t> id_to_index;    // 结点id到内部索引的映射
    std::unordered_map<uint32_t, NodeOffset> offset_map; // 结点id到文件偏移的映射（优化）
    bool optimized = false; // 是否使用优化
    std::string graph_file_path; // 图文件路径（优化）
    uint32_t entrypoint = 0;
    size_t max_level = 0;

    bool load_from_file(const std::string& path, bool optimized = false);

    std::vector<std::pair<uint32_t,float>> search_candidates(const HNSWGraph& g, const std::string& storage_url, const std::vector<float>& query,
    uint32_t entry_id, size_t ef, size_t k) const;
    float l2_sq(const std::vector<float>& a, const std::vector<float>& b) const;
    std::vector<float> fetch_vector(const std::string& storage_url, uint32_t id) const;
    std::vector<uint32_t> load_neighbors(uint32_t id) const;
};
