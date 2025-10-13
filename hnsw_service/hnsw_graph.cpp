// #include "hnsw_graph.h"
// #include <fstream>
// #include <iostream>
// #include <queue>
// #include <unordered_set>
// #include <algorithm>
// #include "../httplib.h"
// #include <../nlohmann/json.hpp>

// using json = nlohmann::json;

// bool HNSWGraph::load_from_file(const std::string& path, bool optimized) 
// {
//     this->optimized = optimized;
//     graph_file_path = path;

//     std::ifstream in(path, std::ios::binary);
//     if (!in) { std::cerr<<"Failed to open graph file: "<<path<<"\n"; return false; }

//     // uint32_t node_count;
//     // in.read(reinterpret_cast<char*>(&entrypoint), sizeof(entrypoint));
//     // in.read(reinterpret_cast<char*>(&max_level), sizeof(max_level));
//     // in.read(reinterpret_cast<char*>(&node_count), sizeof(node_count));
//     uint32_t node_count_u32 = 0;
//     uint32_t entrypoint_u32 = 0;
//     uint32_t max_level_u32 = 0;

//     // 注意读取顺序要与 saveIndex 保持一致（你用 xxd 已确认为 entrypoint, max_level, node_count）
//     in.read(reinterpret_cast<char*>(&entrypoint_u32), sizeof(entrypoint_u32));
//     in.read(reinterpret_cast<char*>(&max_level_u32), sizeof(max_level_u32));
//     in.read(reinterpret_cast<char*>(&node_count_u32), sizeof(node_count_u32));

//     // 将 32-bit 值安全地转换给 class 中的变量（size_t / uint32_t）
//     entrypoint = entrypoint_u32;
//     max_level = static_cast<size_t>(max_level_u32);
//     uint32_t node_count = node_count_u32;

//     adjacency.clear();
//     adjacency.resize(node_count);
//     id_to_index.clear();
//     offset_map.clear();

//     for (uint32_t i=0;i<node_count;i++){
//         uint32_t id;
//         in.read(reinterpret_cast<char*>(&id), sizeof(id));
//         uint32_t levels;
//         in.read(reinterpret_cast<char*>(&levels), sizeof(levels));
//         // 读取level 0的邻居
//         for (uint32_t l=0; l<levels; ++l) {
//             uint32_t deg;
//             // uint64_t offset_before = (uint64_t)in.tellg();
//             std::streampos deg_pos = in.tellg();
//             in.read(reinterpret_cast<char*>(&deg), sizeof(deg));
//             if(!this->optimized)
//             {
//                 std::vector<uint32_t> neigh(deg);
//                 for (uint32_t d=0; d<deg; ++d) in.read(reinterpret_cast<char*>(&neigh[d]), sizeof(uint32_t));
//                 if (l==0) adjacency[i] = std::move(neigh);
//             }
//             else
//             {
                
//                 // 仅记录level 0的偏移和度数
//                 if(l==0)
//                 {
//                     NodeOffset info;
//                     info.offset = static_cast<uint64_t>(deg_pos) + sizeof(uint32_t);
//                     info.degree = deg;
//                     offset_map[id] = info;
//                 }

//                 // 跳过邻接表
//                 in.seekg(sizeof(uint32_t) * deg, std::ios::cur);
//             }
           
//         }
//         id_to_index[id] = i;
//     }

//     std::cout<<"Loaded HNSW graph: nodes="<<node_count<<", entry="<<entrypoint<<", max_level="<<max_level;
//     if (optimized) std::cout<<" [memory optimized]";
//     std::cout<<"\n";
//     return true;
// }

// std::vector<std::pair<uint32_t, float>> HNSWGraph::search_candidates(const HNSWGraph &g, const std::string &storage_url, const std::vector<float> &query, uint32_t entry_id, size_t ef, size_t k) const
// {
   
//     std::vector<std::pair<uint32_t,float>> result;
//     std::queue<uint32_t> q;
//     std::unordered_set<uint32_t> seen;

//     q.push(entry_id);
//     seen.insert(entry_id);
//     std::vector<uint32_t> cand_ids;

//     while (!q.empty() && cand_ids.size() < ef) {
//         uint32_t cur = q.front(); q.pop();
//         cand_ids.push_back(cur);

//         auto it = g.id_to_index.find(cur);
//         if (it == g.id_to_index.end()) continue;

//         if (g.optimized) {
//             // 优化模式：从文件读邻居（返回临时 vector）
//             auto neis = g.load_neighbors(cur);
//             for (auto nb: neis) {
//                 if (seen.insert(nb).second) q.push(nb);
//             }
//         } else {
//             // 普通模式：直接引用内存中的邻接向量
//             const auto &neis = g.adjacency[it->second];
//             for (auto nb: neis) {
//                 if (seen.insert(nb).second) q.push(nb);
//             }
//         }
//     }

//     for (auto id: cand_ids) {
//         try {
//             auto vec = fetch_vector(storage_url, id);
//             float dist = l2_sq(query, vec);
//             result.emplace_back(id, dist);
//         } catch (...) {
//             // skip missing
//         }
//     }
//     std::sort(result.begin(), result.end(), [](auto &a, auto &b){ return a.second < b.second; });
//     if (result.size() > k) result.resize(k);
//     return result;
// }

// //计算距离
// float HNSWGraph::l2_sq(const std::vector<float> &a, const std::vector<float> &b) const
// {
//     float s = 0.0f;
//     size_t n = a.size();
//     for (size_t i=0;i<n;i++){ float d = a[i]-b[i]; s += d*d; }
//     return s;
// }

// //获取向量
// std::vector<float> HNSWGraph::fetch_vector(const std::string &storage_url, uint32_t id) const
// {
//     httplib::Client cli(storage_url.c_str());
//     auto res = cli.Get((std::string("/vec/get?id=") + std::to_string(id)).c_str());
//     if (!res || res->status != 200) throw std::runtime_error("fetch_vector failed for id=" + std::to_string(id));

//     json j = json::parse(res->body);
//     return j["values"].get<std::vector<float>>();
// }

// std::vector<uint32_t> HNSWGraph::load_neighbors(uint32_t id) const
// {
//     auto it = offset_map.find(id);
//     if (it == offset_map.end()) return {};
//     if (graph_file_path.empty()) return {};

//     std::ifstream in(graph_file_path, std::ios::binary);
//     if (!in) return {};

//     const NodeOffset &info = it->second;
//     in.seekg(static_cast<std::streamoff>(info.offset), std::ios::beg);
//     std::vector<uint32_t> neigh(info.degree);
//     if (info.degree > 0) {
//         in.read(reinterpret_cast<char*>(neigh.data()), sizeof(uint32_t) * info.degree);
//     }
//     return neigh;
// }

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
        if (in.eof()) {
            std::cerr << "Unexpected EOF at node " << i << std::endl;
            return false;
        }

        uint32_t id;
        if (!in.read(reinterpret_cast<char*>(&id), sizeof(id))) {
            std::cerr << "Failed to read node id at index " << i << std::endl;
            return false;
        }

        uint32_t levels;
        if (!in.read(reinterpret_cast<char*>(&levels), sizeof(levels))) {
            std::cerr << "Failed to read levels for node " << id << std::endl;
            return false;
        }

        id_to_index[id] = i;

        // 为每个层级记录正确的文件偏移
        for (uint32_t l = 0; l < levels; ++l) {
            
            std::streampos neighbor_start_pos = in.tellg();
            
            uint32_t deg;
            if (!in.read(reinterpret_cast<char*>(&deg), sizeof(deg))) {
                std::cerr << "Failed to read degree for node " << id << " level " << l << std::endl;
                return false;
            }

            if (deg > 1000000) {
                std::cerr << "Suspicious degree " << deg << " for node " << id << " level " << l << std::endl;
                return false;
            }

            if (!optimized) {
                // 普通模式：读取所有邻居到内存
                std::vector<uint32_t> neigh(deg);
                for (uint32_t d = 0; d < deg; ++d) {
                    if (!in.read(reinterpret_cast<char*>(&neigh[d]), sizeof(uint32_t))) {
                        std::cerr << "Failed to read neighbor for node " << id << std::endl;
                        return false;
                    }
                }

                if (l == 0) {
                    adjacency[i] = neigh;
                }

                if (l < levels_neighbors.size()) {
                    levels_neighbors[l][id] = neigh;
                }
            } else {
                
                if (l == 0) { 
                    NodeOffset info;
                   
                    info.offset = static_cast<uint64_t>(neighbor_start_pos) + sizeof(uint32_t);
                    info.degree = deg;
                    level_offsets[l][id] = info;
                }

                // 跳过邻接表数据
                in.seekg(sizeof(uint32_t) * deg, std::ios::cur);
                if (in.fail()) {
                    std::cerr << "Failed to skip neighbors for node " << id << std::endl;
                    return false;
                }
            }
        }

        if ((i + 1) % 10000 == 0) {
            std::cout << "Loaded " << (i + 1) << "/" << node_count << " nodes" << std::endl;
        }
    }

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

void HNSWGraph::initialize_http_client(const std::string& storage_url) const {
    if (!http_client) {
        http_client = std::make_unique<httplib::Client>(storage_url.c_str());
        http_client->set_connection_timeout(5);
        http_client->set_read_timeout(10);
        http_client->set_write_timeout(5);
    }
}

std::vector<float> HNSWGraph::fetch_vector(const std::string& storage_url, uint32_t id) const 
{
    auto it = vector_cache.find(id);
    if (it != vector_cache.end()) 
    {
        return it->second;
    }

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
            vector_cache[id] = j["values"].get<std::vector<float>>();
            return vector_cache[id];

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

// std::vector<uint32_t> HNSWGraph::load_neighbors(uint32_t id) const {
//     首先检查缓存
//     if (auto cached = neighbors_cache.get(id)) {
//         return *cached;
//     }

//     if (!optimized) {
//         auto it = id_to_index.find(id);
//         if (it != id_to_index.end() && it->second < adjacency.size()) {
//             std::vector<uint32_t> result = adjacency[it->second];
//             neighbors_cache.put(id, result);
//             return result;
//         }
//         return {};
//     }

//     if (!file_stream || !file_stream->is_open()) {
//         return {};
//     }

//     auto it = offset_map.find(id);
//     if (it == offset_map.end()) {
//         return {};
//     }

//     const NodeOffset& info = it->second;
//     file_stream->clear();
//     file_stream->seekg(static_cast<std::streamoff>(info.offset), std::ios::beg);
    
//     if (file_stream->fail()) {
//         std::cerr << "Failed to seek to offset " << info.offset << " for node " << id << std::endl;
//         return {};
//     }

//     std::vector<uint32_t> neigh(info.degree);
//     if (info.degree > 0) {
//         file_stream->read(reinterpret_cast<char*>(neigh.data()), sizeof(uint32_t) * info.degree);
//         if (file_stream->fail()) {
//             std::cerr << "Failed to read neighbors for node " << id << std::endl;
//             return {};
//         }
//     }
    
//     // 存入缓存
//     neighbors_cache.put(id, neigh);
//     return neigh;
// }

std::vector<uint32_t> HNSWGraph::get_neighbors(uint32_t id, int level) const {
    // if (level == 0) {
    //     return load_neighbors(id);
    // }
    
    // if (!optimized && level < levels_neighbors.size()) {
    //     auto it = levels_neighbors[level].find(id);
    //     if (it != levels_neighbors[level].end()) {
    //         return it->second;
    //     }
    // }
    
    // return {};
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
        return {};
    }
    
    const auto& offset_map = level_offsets[level];
    auto it = offset_map.find(id);
    if (it == offset_map.end()) {
        return {};  // 该节点在该层没有邻居
    }
    
    // 使用LRU缓存
    uint64_t cache_key = (static_cast<uint64_t>(id) << 32) | level;
    if (auto cached = neighbors_cache.get(cache_key)) {
        return *cached;
    }
    
    // 从文件读取
    const NodeOffset& info = it->second;
    if (!file_stream || !file_stream->is_open()) {
        return {};
    }
    
    file_stream->clear();
    file_stream->seekg(static_cast<std::streamoff>(info.offset), std::ios::beg);
    
    if (file_stream->fail()) {
        return {};
    }
    
    std::vector<uint32_t> neigh(info.degree);
    if (info.degree > 0) {
        file_stream->read(reinterpret_cast<char*>(neigh.data()), sizeof(uint32_t) * info.degree);
        if (file_stream->fail()) {
            return {};
        }
    }
    
    neighbors_cache.put(cache_key, neigh);
    return neigh;
}

float HNSWGraph::l2_sq(const std::vector<float>& a, const std::vector<float>& b) const {
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
        
        bool changed;
        do {
            changed = false;
            auto neighbors = get_neighbors(current_node, level);
            
            for (uint32_t neighbor : neighbors) {
                try {
                    auto neighbor_vec = fetch_vector(storage_url, neighbor);
                    float neighbor_dist = l2_sq(query, neighbor_vec);
                    
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

std::vector<std::pair<uint32_t, float>> HNSWGraph::search_base_layer_original(
    const std::string& storage_url,
    const std::vector<float>& query,
    uint32_t entry_point, size_t ef, size_t k) const {
    
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
        candidates.push({entry_dist, entry_point});
        results.push({entry_dist, entry_point});
        visited.insert(entry_point);
    } catch (...) {
        return {};
    }
    
    float worst_dist = results.top().first;
    
    while (!candidates.empty()) {
        auto [dist, node] = candidates.top();
        
        // 如果当前候选比结果集中最差的结果还差，停止搜索
        if (dist > worst_dist) {
            break;
        }
        
        candidates.pop();
        
        auto neighbors = get_neighbors(node, 0);
        for (uint32_t neighbor : neighbors) {
            if (visited.insert(neighbor).second) {
                try {
                    auto neighbor_vec = fetch_vector(storage_url, neighbor);
                    float neighbor_dist = l2_sq(query, neighbor_vec);
                    
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
                } catch (...) {
                    // 跳过无法获取的节点
                }
            }
        }
    }
    
    // 提取并排序最终结果
    std::vector<std::pair<uint32_t, float>> final_results;
    while (!results.empty()) {
        final_results.push_back(results.top());
        results.pop();
    }
    
    // 按距离升序排序
    std::sort(final_results.begin(), final_results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    if (final_results.size() > k) {
        final_results.resize(k);
    }
    
    return final_results;
}

std::vector<std::pair<uint32_t, float>> HNSWGraph::search_candidates(
    const HNSWGraph& g, const std::string& storage_url, 
    const std::vector<float>& query, uint32_t entry_id, 
    size_t ef, size_t k) const {
    
    if (g.max_level == 0) {
        return search_base_layer_original(storage_url, query, entry_id, ef, k);
    }
    
    // 符合原始HNSW算法的分层搜索
    uint32_t current_entry = entry_id;
    
    // 从最高层开始贪心下降
    for (int level = static_cast<int>(g.max_level); level > 0; --level) {
        current_entry = search_layer_original(storage_url, query, current_entry, level, 1);
    }
    
    // 在底层进行精细搜索
    return search_base_layer_original(storage_url, query, current_entry, ef, k);
}