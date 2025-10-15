#include "hnsw_graph.h"
#include <fstream>
#include <iostream>
#include <queue>
#include <unordered_set>
#include <algorithm>
// #include <thread>
#include <chrono>
#include "../httplib.h"
#include <../nlohmann/json.hpp>

using json = nlohmann::json;

bool HNSWGraph::load_from_file(const std::string& path, bool optimized) 
{
    this->optimized = optimized;
    graph_file_path = path;

    std::ifstream in(path, std::ios::binary);
    if (!in) { 
        std::cerr << "Failed to open graph file: " << path << "\n"; 
        return false; 
    }

    // 读取文件头
    uint32_t entrypoint_u32 = 0, max_level_u32 = 0, node_count_u32 = 0;
    
    if (!in.read(reinterpret_cast<char*>(&entrypoint_u32), sizeof(entrypoint_u32)) ||
        !in.read(reinterpret_cast<char*>(&max_level_u32), sizeof(max_level_u32)) ||
        !in.read(reinterpret_cast<char*>(&node_count_u32), sizeof(node_count_u32))) {
        std::cerr << "Failed to read graph header" << std::endl;
        return false;
    }

    entrypoint = entrypoint_u32;
    max_level = static_cast<size_t>(max_level_u32);
    uint32_t node_count = node_count_u32;

    std::cout << "Loading HNSW graph: nodes=" << node_count 
              << ", entry=" << entrypoint << ", max_level=" << max_level << std::endl;
    std::cout << "Header ends at position: " << in.tellg() << std::endl;

    // 初始化数据结构
    adjacency.clear();
    adjacency.resize(node_count);
    id_to_index.clear();
    level_offsets.clear();
    levels_neighbors.clear();
    level_offsets.resize(max_level + 1);
    levels_neighbors.resize(max_level + 1);

    // 读取节点数据
    for (uint32_t i = 0; i < node_count; i++) {
        std::cout << "=== Reading node " << i << " at position: " << in.tellg() << " ===" << std::endl;

        if (in.eof()) {
            std::cerr << "Unexpected EOF at node " << i << std::endl;
            return false;
        }

        // 读取节点ID
        uint32_t id;
        if (!in.read(reinterpret_cast<char*>(&id), sizeof(id))) {
            std::cerr << "Failed to read node id at index " << i << std::endl;
            return false;
        }
        std::cout << "Node ID: " << id << " (read from position: " << (in.tellg() - static_cast<std::streamoff>(sizeof(id))) << ")" << std::endl;

        // 读取层级数量
        uint32_t levels;
        if (!in.read(reinterpret_cast<char*>(&levels), sizeof(levels))) {
            std::cerr << "Failed to read levels for node " << id << std::endl;
            return false;
        }
        std::cout << "Levels: " << levels << " (read from position: " << (in.tellg() - static_cast<std::streamoff>(sizeof(levels))) << ")" << std::endl;

        id_to_index[id] = i;

        // 读取每一层
        for (uint32_t l = 0; l < levels; ++l) {
            std::cout << "--- Level " << l << " starts at position: " << in.tellg() << " ---" << std::endl;
            
            // 在读取degree之前记录位置
            std::streampos level_start_pos = in.tellg();
            
            // 读取degree
            uint32_t deg;
            if (!in.read(reinterpret_cast<char*>(&deg), sizeof(deg))) {
                std::cerr << "Failed to read degree for node " << id << " level " << l << std::endl;
                return false;
            }
            std::cout << "Degree: " << deg << " (read from position: " << level_start_pos << ")" << std::endl;

            // 邻居数据开始位置（degree之后）
            std::streampos neighbors_start_pos = in.tellg();
            std::cout << "Neighbors start at position: " << neighbors_start_pos << std::endl;

            if (!optimized) {
                // 普通模式：读取邻居到内存
                std::vector<uint32_t> neigh(deg);
                for (uint32_t d = 0; d < deg; ++d) {
                    if (!in.read(reinterpret_cast<char*>(&neigh[d]), sizeof(uint32_t))) {
                        std::cerr << "Failed to read neighbor for node " << id << std::endl;
                        return false;
                    }
                    std::cout << "  Neighbor " << d << ": " << neigh[d] 
                              << " (read from position: " << (neighbors_start_pos + static_cast<std::streamoff>(d * sizeof(uint32_t))) << ")" << std::endl;
                }

                if (l == 0) adjacency[i] = neigh;
                if (l < levels_neighbors.size()) levels_neighbors[l][id] = neigh;
                
            } else {
                // 优化模式：记录文件偏移
                if (l == 0) {
                    NodeOffset info;
                    info.offset = static_cast<uint64_t>(neighbors_start_pos);
                    info.degree = deg;
                    level_offsets[l][id] = info;
                    std::cout << "Stored offset: " << info.offset << " for node " << id << std::endl;
                }

                // 跳过邻居数据
                in.seekg(sizeof(uint32_t) * deg, std::ios::cur);
                if (in.fail()) {
                    std::cerr << "Failed to skip neighbors for node " << id << std::endl;
                    return false;
                }
                std::cout << "Skipped " << deg << " neighbors, now at position: " << in.tellg() << std::endl;
            }
        }

        std::cout << "Node " << id << " completed, next node at position: " << in.tellg() << std::endl;
    }

    // 优化模式的文件流初始化
    if (optimized) {
        file_stream = std::make_unique<std::ifstream>(graph_file_path, std::ios::binary);
        if (!file_stream->is_open()) {
            std::cerr << "Failed to open graph file for optimized mode: " << graph_file_path << std::endl;
            return false;
        }
    }

    std::cout << "Successfully loaded HNSW graph: nodes=" << node_count 
              << ", entry=" << entrypoint << ", max_level=" << max_level;
    if (optimized) std::cout << " [memory optimized]";
    std::cout << std::endl;
    
    return true;
}

void HNSWGraph::initialize_http_client(const std::string& storage_url) const 
{
    if (!http_client) {
        http_client = std::make_unique<httplib::Client>(storage_url.c_str());
        http_client->set_connection_timeout(5);
        http_client->set_read_timeout(10);
        http_client->set_write_timeout(5);

        // 测试连接
        std::cout << "DEBUG: 初始化HTTP客户端连接到: " << storage_url << std::endl;
        auto test_res = http_client->Get("/vec/get?id=0");
        if (test_res) {
            std::cout << "DEBUG: HTTP客户端连接测试: 状态 " << test_res->status << std::endl;
        } else {
            std::cerr << "DEBUG: HTTP客户端连接测试失败" << std::endl;
        }
    }

   
}

std::vector<float> HNSWGraph::fetch_vector(const std::string& storage_url, uint32_t id) const 
{
    // auto it = vector_cache.find(id);
    // if (it != vector_cache.end()) 
    // {
    //     return it->second;
    // }

    initialize_http_client(storage_url);
    
    const int max_retries = 3;
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        try {
            auto res = http_client->Get("/vec/get?id=" + std::to_string(id));
            
            if (!res) {
                throw std::runtime_error("HTTP request failed");
            }
            
            if (res->status != 200) {
                throw std::runtime_error("HTTP status " + std::to_string(res->status));
            }
            
            json j = json::parse(res->body);
            // vector_cache[id] = j["values"].get<std::vector<float>>();
            // return vector_cache[id];
            return j["values"].get<std::vector<float>>();

        } catch (const std::exception& e) {
            if (attempt == max_retries - 1) {
                throw std::runtime_error("fetch_vector failed for id=" + std::to_string(id) + 
                                       " after " + std::to_string(max_retries) + " attempts: " + e.what());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
        }
    }
    
    throw std::runtime_error("Max retries exceeded for id=" + std::to_string(id));
}


std::vector<uint32_t> HNSWGraph::get_neighbors(uint32_t id, int level) const 
{
    if (!optimized) {
        // 普通模式的逻辑不变
        if (level == 0) {
            auto it = id_to_index.find(id);
            if (it != id_to_index.end()) {
                return adjacency[it->second];
            }
        } else if (level < levels_neighbors.size()) {
            auto it = levels_neighbors[level].find(id);
            if (it != levels_neighbors[level].end()) {
                return it->second;
            }
        }
        return {};
    }
    
    // 优化模式：从文件读取指定层的邻居
    if (level < 0 || level >= level_offsets.size()) {
        // std::cout << "DEBUG: Invalid level" << std::endl;
        return {};
    }
    
    const auto& offset_map = level_offsets[level];
    auto it = offset_map.find(id);
    if (it == offset_map.end()) {
        // std::cout << "DEBUG: No offset found for node " << id << " at level " << level << std::endl;
        return {};  // 该节点在该层没有邻居
    }
    
    // // 使用LRU缓存
    // uint64_t cache_key = (static_cast<uint64_t>(id) << 32) | level;
    // if (auto cached = neighbors_cache.get(cache_key)) {
    //     std::cout << "DEBUG: Using cached neighbors for node " << id << std::endl;
    //     return *cached;
    // }
    
    // 从文件读取
    const NodeOffset& info = it->second;
    if (!file_stream || !file_stream->is_open()) {
        // std::cout << "DEBUG: File stream not open" << std::endl;
        return {};
    }
    
    file_stream->clear();
    file_stream->seekg(static_cast<std::streamoff>(info.offset), std::ios::beg);
    
    if (file_stream->fail()) {
        // std::cout << "DEBUG: Seek failed" << std::endl;  
        return {};
    }
    
    std::vector<uint32_t> neigh(info.degree);
    if (info.degree > 0) {
        file_stream->read(reinterpret_cast<char*>(neigh.data()), sizeof(uint32_t) * info.degree);
        if (file_stream->fail()) {
            // std::cout << "DEBUG: Read failed" << std::endl;
            return {};
        }
    }
    if (file_stream->gcount() != sizeof(uint32_t) * info.degree) {
            std::cerr << "DEBUG: Read incomplete for node " << id 
                      << ", expected " << (sizeof(uint32_t) * info.degree) 
                      << " bytes, got " << file_stream->gcount() << " bytes" << std::endl;
            return {};
        }
    return neigh;
}

float HNSWGraph::l2_sq(const std::vector<float>& a, const std::vector<float>& b) const 
{
    if (a.size() != b.size()) {
        throw std::invalid_argument("Vector dimension mismatch: " + 
                                   std::to_string(a.size()) + " vs " + std::to_string(b.size()));
    }
    
    float s = 0.0f;
    size_t n = a.size();
    for (size_t i = 0; i < n; i++) {
        float d = a[i] - b[i];
        s += d * d;
    }
    return s;
}

uint32_t HNSWGraph::search_layer_original(const std::string& storage_url,
                                         const std::vector<float>& query,
                                         uint32_t entry_point, 
                                         int level, size_t ef) const {
    uint32_t current_node = entry_point;
    
    try {
        auto current_vec = fetch_vector(storage_url, current_node);
        float current_dist = l2_sq(query, current_vec);
        std::cout << "DEBUG SEARCH: Starting at entry point " << current_node 
                  << " with initial distance " << current_dist << std::endl;
        bool changed;
        do {
            changed = false;
            auto neighbors = get_neighbors(current_node, level);
            
            for (uint32_t neighbor : neighbors) {
                try {
                    auto neighbor_vec = fetch_vector(storage_url, neighbor);
                    float neighbor_dist = l2_sq(query, neighbor_vec);
                    std::cout << "DEBUG SEARCH: Checking neighbor " << neighbor 
                              << " with distance " << neighbor_dist << std::endl;
                    if (neighbor_dist < current_dist) {
                        current_node = neighbor;
                        current_dist = neighbor_dist;
                        changed = true;
                        break; // 找到更近的，重新开始搜索
                    }
                } catch (...) {
                    // 跳过无法获取的节点
                }
            }
        } while (changed);
        
    } catch (...) {
        // 如果入口点获取失败，返回原入口点
    }
    
    return current_node;
}

std::vector<std::pair<uint32_t, float>> HNSWGraph::search_base_layer_original(const std::string& storage_url, const std::vector<float>& query,
    uint32_t entry_point, size_t ef, size_t k) const 
{
    
    using NodeDist = std::pair<float, uint32_t>;
    
    // 候选集（最小堆，用于按距离扩展）
    auto cmp_min = [](const NodeDist& a, const NodeDist& b) { return a.first > b.first; };
    std::priority_queue<NodeDist, std::vector<NodeDist>, decltype(cmp_min)> candidates(cmp_min);
    
    // 结果集（最大堆，维护ef个最佳候选）
    auto cmp_max = [](const NodeDist& a, const NodeDist& b) { return a.first < b.first; };
    std::priority_queue<NodeDist, std::vector<NodeDist>, decltype(cmp_max)> results(cmp_max);
    
    std::unordered_set<uint32_t> visited;
    
    try {
        auto entry_vec = fetch_vector(storage_url, entry_point);
        float entry_dist = l2_sq(query, entry_vec);
       
        std::cout << "DEBUG SEARCH: Entry point " << entry_point << std::endl;
        std::cout << "DEBUG SEARCH: Entry vector: [" << entry_vec[0] << ", " << entry_vec[1] << ", " << entry_vec[2] << "]" << std::endl;
        std::cout << "DEBUG SEARCH: Query vector: [" << query[0] << ", " << query[1] << ", " << query[2] << "]" << std::endl;
        std::cout << "DEBUG SEARCH: Entry distance: " << entry_dist << std::endl;

        candidates.push({entry_dist, entry_point});
        results.push({entry_dist, entry_point});
        visited.insert(entry_point);
    } catch (const std::exception& e) {
        std::cerr << "DEBUG SEARCH: Failed to initialize: " << e.what() << std::endl;
        return {};
    }
    
    float worst_dist = results.top().first;
    
    size_t iteration = 0;
    while (!candidates.empty()) {
        auto [dist, node] = candidates.top();
        
        // 如果当前候选比结果集中最差的结果还差，停止搜索
        // if (dist > worst_dist) {
        //     break;
        // }
        
        candidates.pop();
        
         std::cout << "DEBUG SEARCH: Iteration " << iteration 
                  << ", processing node " << node 
                  << " with distance " << dist << std::endl;

        auto neighbors = get_neighbors(node, 0);
        for (uint32_t neighbor : neighbors) {
            if (visited.insert(neighbor).second) {
                try {
                    auto neighbor_vec = fetch_vector(storage_url, neighbor);
                    float neighbor_dist = l2_sq(query, neighbor_vec);
                    
                    std::cout << "DEBUG SEARCH: Neighbor " << neighbor 
                          << ", vector: [" << neighbor_vec[0] << ", " << neighbor_vec[1] << ", " << neighbor_vec[2] << "]"
                          << ", distance: " << neighbor_dist << std::endl;

                    // 符合HNSW原始算法：如果候选集未满或距离小于最差结果，则加入
                    if (results.size() < ef || neighbor_dist < worst_dist) {
                        candidates.push({neighbor_dist, neighbor});
                        results.push({neighbor_dist, neighbor});
                        
                        // 维护结果集大小
                        if (results.size() > ef) {
                            results.pop();
                        }
                        
                        // 更新最差距离
                        worst_dist = results.top().first;
                    }
                } catch (const std::exception& e) {
                    // 跳过无法获取的节点
                    std::cerr << "DEBUG SEARCH: Failed to process neighbor " << neighbor << ": " << e.what() << std::endl;
                }
            }
        }
    }
    
    // 提取并排序最终结果
    std::vector<std::pair<uint32_t, float>> final_results;
    while (!results.empty()) {
        auto [dist, id] = results.top();
        // final_results.push_back(results.top());
        final_results.emplace_back(id, dist);
        results.pop();
    }
    
    // 按距离升序排序
    std::sort(final_results.begin(), final_results.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    
    if (final_results.size() > k) {
        final_results.resize(k);
    }

    std::cout << "DEBUG SEARCH: Final results:" << std::endl;
    for (size_t i = 0; i < final_results.size(); ++i) {
        std::cout << "  Result " << i << ": id=" << final_results[i].first 
                  << ", dist=" << final_results[i].second << std::endl;
    }
    
    return final_results;
}

std::vector<std::pair<uint32_t, float>> HNSWGraph::search_candidates(const HNSWGraph& g, const std::string& storage_url, const std::vector<float>& query, uint32_t entry_id, 
    size_t ef, size_t k) const 
{
    
    try
    {
        std::cout << "=== DEBUG: 开始分层搜索 ===" << std::endl;
        std::cout << "入口点: " << entry_id << ", ef: " << ef << ", k: " << k << std::endl;
        std::cout << "图最大层级: " << g.max_level << std::endl;

        if (g.max_level == 0) {
            std::cout << "DEBUG: 直接搜索底层" << std::endl;
            return search_base_layer_original(storage_url, query, entry_id, ef, k);
        }
        
        // 符合原始HNSW算法的分层搜索
        uint32_t current_entry = entry_id;
        
        // 从最高层开始贪心下降
        for (int level = static_cast<int>(g.max_level); level > 0; --level) {
            std::cout << "DEBUG: 搜索层级 " << level << std::endl;
            current_entry = search_layer_original(storage_url, query, current_entry, level, 1);
            std::cout << "DEBUG: 层级 " << level << " 搜索完成，当前入口: " << current_entry << std::endl;
        }
        
        // 在底层进行精细搜索
        std::cout << "DEBUG: 开始底层精细搜索" << std::endl;
        return search_base_layer_original(storage_url, query, current_entry, ef, k);
    }
    catch (const std::exception& e) {
       std::cerr << "搜索过程中发生异常: " << e.what() << std::endl;
        return {};
    }
    catch (...) {
        std::cerr << "搜索过程中发生未知异常" << std::endl;
        return {};
    }
}