#include "hnsw_graph.h"
#include <fstream>
#include <iostream>
#include <queue>
#include <unordered_set>
#include <algorithm>
#include "../httplib.h"
#include <../nlohmann/json.hpp>

using json = nlohmann::json;

bool HNSWGraph::load_from_file(const std::string& path, bool optimized) 
{
    this->optimized = optimized;
    graph_file_path = path;

    std::ifstream in(path, std::ios::binary);
    if (!in) { std::cerr<<"Failed to open graph file: "<<path<<"\n"; return false; }

    uint32_t node_count;
    in.read(reinterpret_cast<char*>(&node_count), sizeof(node_count));
    in.read(reinterpret_cast<char*>(&entrypoint), sizeof(entrypoint));
    in.read(reinterpret_cast<char*>(&max_level), sizeof(max_level));

    adjacency.clear();
    adjacency.resize(node_count);
    id_to_index.clear();
    offset_map.clear();

    for (uint32_t i=0;i<node_count;i++){
        uint32_t id;
        in.read(reinterpret_cast<char*>(&id), sizeof(id));
        uint32_t levels;
        in.read(reinterpret_cast<char*>(&levels), sizeof(levels));
        // 读取level 0的邻居
        for (uint32_t l=0; l<levels; ++l) {
            uint32_t deg;
            // uint64_t offset_before = (uint64_t)in.tellg();
            std::streampos deg_pos = in.tellg();
            in.read(reinterpret_cast<char*>(&deg), sizeof(deg));
            if(!this->optimized)
            {
                std::vector<uint32_t> neigh(deg);
                for (uint32_t d=0; d<deg; ++d) in.read(reinterpret_cast<char*>(&neigh[d]), sizeof(uint32_t));
                if (l==0) adjacency[i] = std::move(neigh);
            }
            else
            {
                
                // 仅记录level 0的偏移和度数
                if(l==0)
                {
                    NodeOffset info;
                    info.offset = static_cast<uint64_t>(deg_pos) + sizeof(uint32_t);
                    info.degree = deg;
                    offset_map[id] = info;
                }

                // 跳过邻接表
                in.seekg(sizeof(uint32_t) * deg, std::ios::cur);
            }
           
        }
        id_to_index[id] = i;
    }

    std::cout<<"Loaded HNSW graph: nodes="<<node_count<<", entry="<<entrypoint<<", max_level="<<max_level;
    if (optimized) std::cout<<" [memory optimized]";
    std::cout<<"\n";
    return true;
}

std::vector<std::pair<uint32_t, float>> HNSWGraph::search_candidates(const HNSWGraph &g, const std::string &storage_url, const std::vector<float> &query, uint32_t entry_id, size_t ef, size_t k) const
{
    // std::vector<std::pair<uint32_t,float>> result;
    // std::queue<uint32_t> q;
    // std::unordered_set<uint32_t> seen;
    
    // q.push(entry_id);
    // seen.insert(entry_id);
    // std::vector<uint32_t> cand_ids;
    // while (!q.empty() && cand_ids.size() < ef) {
    //     uint32_t cur = q.front(); q.pop();
    //     cand_ids.push_back(cur);
    //     auto it = g.id_to_index.find(cur);
    //     if (it == g.id_to_index.end()) continue;
    //     // const auto &neis = g.adjacency[it->second];
    //     const auto &neis = g.optimized ? g.load_neighbors(cur) : g.adjacency[it->second];
    //     for (auto nb: neis) {
    //         if (seen.insert(nb).second) q.push(nb);
    //     }
    // }
    // // batch fetch via storage service endpoint /vec/batch_get if available
    // // This implementation uses individual fetch; for production use batch endpoint.
    // for (auto id: cand_ids) {
    //     try {
    //         auto vec = fetch_vector(storage_url, id);
    //         float dist = l2_sq(query, vec);
    //         result.emplace_back(id, dist);
    //     } catch (...) {
    //         // skip missing
    //     }
    // }
    // std::sort(result.begin(), result.end(), [](auto &a, auto &b){ return a.second < b.second; });
    // if (result.size() > k) result.resize(k);
    // return result;
    std::vector<std::pair<uint32_t,float>> result;
    std::queue<uint32_t> q;
    std::unordered_set<uint32_t> seen;

    q.push(entry_id);
    seen.insert(entry_id);
    std::vector<uint32_t> cand_ids;

    while (!q.empty() && cand_ids.size() < ef) {
        uint32_t cur = q.front(); q.pop();
        cand_ids.push_back(cur);

        auto it = g.id_to_index.find(cur);
        if (it == g.id_to_index.end()) continue;

        if (g.optimized) {
            // 优化模式：从文件读邻居（返回临时 vector）
            auto neis = g.load_neighbors(cur);
            for (auto nb: neis) {
                if (seen.insert(nb).second) q.push(nb);
            }
        } else {
            // 普通模式：直接引用内存中的邻接向量
            const auto &neis = g.adjacency[it->second];
            for (auto nb: neis) {
                if (seen.insert(nb).second) q.push(nb);
            }
        }
    }

    for (auto id: cand_ids) {
        try {
            auto vec = fetch_vector(storage_url, id);
            float dist = l2_sq(query, vec);
            result.emplace_back(id, dist);
        } catch (...) {
            // skip missing
        }
    }
    std::sort(result.begin(), result.end(), [](auto &a, auto &b){ return a.second < b.second; });
    if (result.size() > k) result.resize(k);
    return result;
}

//计算距离
float HNSWGraph::l2_sq(const std::vector<float> &a, const std::vector<float> &b) const
{
    float s = 0.0f;
    size_t n = a.size();
    for (size_t i=0;i<n;i++){ float d = a[i]-b[i]; s += d*d; }
    return s;
}

//获取向量
std::vector<float> HNSWGraph::fetch_vector(const std::string &storage_url, uint32_t id) const
{
    httplib::Client cli(storage_url.c_str());
    auto res = cli.Get((std::string("/vec/get?id=") + std::to_string(id)).c_str());
    if (!res || res->status != 200) throw std::runtime_error("fetch_vector failed for id=" + std::to_string(id));

    json j = json::parse(res->body);
    return j["values"].get<std::vector<float>>();
}

std::vector<uint32_t> HNSWGraph::load_neighbors(uint32_t id) const
{
    auto it = offset_map.find(id);
    if (it == offset_map.end()) return {};
    if (graph_file_path.empty()) return {};

    std::ifstream in(graph_file_path, std::ios::binary);
    if (!in) return {};

    const NodeOffset &info = it->second;
    in.seekg(static_cast<std::streamoff>(info.offset), std::ios::beg);
    std::vector<uint32_t> neigh(info.degree);
    if (info.degree > 0) {
        in.read(reinterpret_cast<char*>(neigh.data()), sizeof(uint32_t) * info.degree);
    }
    return neigh;
}

// BFS候选搜索
// std::vector<uint32_t> HNSWGraph::search_candidates(uint32_t entrypoint, size_t ef) 
// {
//     std::vector<uint32_t> res;
//     std::queue<uint32_t> q;
//     std::unordered_set<uint32_t> seen;// 已访问节点集合

//     q.push(entrypoint);
//     seen.insert(entrypoint);
//     while (!q.empty() && res.size() < ef) 
//     {
//         uint32_t cur = q.front(); 
//         q.pop();
//         res.push_back(cur);
//         // find node in nodes_
//         // linear scan: nodes_ may be large; in production use unordered_map id->index
//         // For simplicity build index map once? We'll linear scan (ok for demo)
//         auto it = std::find_if(nodes_.begin(), nodes_.end(), [&](const HNSWNode& n){ return n.id == cur; });
//         if (it == nodes_.end()) continue;
//         if (it->levels.size() == 0) continue;
//         for (auto nb: it->levels[0]) 
//         {
//             if (seen.insert(nb).second) q.push(nb);
//         }
//     }

//     return res;
// }
