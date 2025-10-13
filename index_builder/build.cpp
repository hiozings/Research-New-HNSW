#include <iostream>
#include <vector>
#include <random>
#include <fstream>
#include <string>
#include <rocksdb/db.h>
#include "../tools/common.h"

// hnswlib includes (assume third_party/hnswlib contains headers)
#include "../hnswlib/hnswlib.h"


// 使用 hnswlib 的类型别名
using tableint = unsigned int;
using linklistsizeint = unsigned int;

// 导出邻接表到二进制文件（格式与问题中一致）
// header: uint32_t entrypoint, uint32_t max_level, uint32_t node_count
// per-node:
//   uint32_t label
//   uint32_t levels
//   for each level:
//     uint32_t deg
//     deg * uint32_t neighbor_labels (外部 id)
void export_adjacency(hnswlib::HierarchicalNSW<float>& appr_alg, const std::string& outpath) {
    // 获取元素数量
    size_t cur_elements = appr_alg.cur_element_count.load();
    if (cur_elements == 0) {
        std::cerr << "export_adjacency: index empty\n";
        return;
    }

    // sanity checks: element_levels_ size etc.
    if (appr_alg.element_levels_.size() < cur_elements) {
        throw std::runtime_error("export_adjacency: element_levels_ too small");
    }

    // 安全地读取 entrypoint 和 maxlevel
    int enterpoint = appr_alg.enterpoint_node_;
    int maxlevel = appr_alg.maxlevel_;

    // open file
    std::ofstream out(outpath, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open adjacency output file");

    uint32_t entry_u = static_cast<uint32_t>(enterpoint < 0 ? 0 : enterpoint);
    uint32_t maxlevel_u = static_cast<uint32_t>(maxlevel < 0 ? 0 : maxlevel);
    uint32_t node_count_u = static_cast<uint32_t>(cur_elements);

    out.write(reinterpret_cast<const char*>(&entry_u), sizeof(entry_u));
    out.write(reinterpret_cast<const char*>(&maxlevel_u), sizeof(maxlevel_u));
    out.write(reinterpret_cast<const char*>(&node_count_u), sizeof(node_count_u));

    // iterate internal indices 0..cur_elements-1
    for (size_t internal_idx = 0; internal_idx < cur_elements; ++internal_idx) {
        // external label
        unsigned int lbl = static_cast<unsigned int>(appr_alg.getExternalLabel(static_cast<tableint>(internal_idx)));
        out.write(reinterpret_cast<const char*>(&lbl), sizeof(lbl));

        // levels for this element (element_levels_ stores level, so number of levels = level+1)
        int lev = appr_alg.element_levels_[internal_idx];
        if (lev < 0) lev = 0;
        uint32_t levels_u = static_cast<uint32_t>(lev + 1);
        out.write(reinterpret_cast<const char*>(&levels_u), sizeof(levels_u));

        // for each level read linklist pointer and degree
        for (int level = 0; level <= lev; ++level) {
            // obtain pointer to linklist at this level
            // use get_linklist_at_level: returns linklistsizeint*
            linklistsizeint *ll = nullptr;
            try {
                ll = appr_alg.get_linklist_at_level(static_cast<tableint>(internal_idx), level);
            } catch (...) {
                // fallback: treat as empty
                uint32_t deg0 = 0;
                out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
                continue;
            }
            // get degree using getListCount
            unsigned short deg_us = appr_alg.getListCount(ll);
            uint32_t deg = static_cast<uint32_t>(deg_us);

            // sanity cap (to avoid reading garbage)
            const uint32_t MAX_REASONABLE_DEG = 1000000;
            if (deg > MAX_REASONABLE_DEG) {
                std::cerr << "export_adjacency: unreasonable deg " << deg
                          << " at internal_idx=" << internal_idx << " level=" << level << "\n";
                // write 0 and skip to be safe
                uint32_t deg0 = 0;
                out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
                continue;
            }

            out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));

            if (deg == 0) continue;

            // neighbor internal ids start at (ll + 1) as tableint*
            tableint *neighbors_ptr = reinterpret_cast<tableint*>(ll + 1);

            for (uint32_t j = 0; j < deg; ++j) {
                tableint nb_internal = neighbors_ptr[j];
                // safety: ensure internal id in range
                if (nb_internal < 0 || static_cast<size_t>(nb_internal) >= cur_elements) {
                    unsigned int nb_label = 0; // invalid -> write 0
                    out.write(reinterpret_cast<const char*>(&nb_label), sizeof(nb_label));
                } else {
                    unsigned int nb_label = static_cast<unsigned int>(appr_alg.getExternalLabel(nb_internal));
                    out.write(reinterpret_cast<const char*>(&nb_label), sizeof(nb_label));
                }
            }
        }

        // for levels > element_levels_[i] we still might want to write 0 entries to keep strict format,
        // but above we only iterate to lev so file is compact.
    }

    out.close();
    std::cerr << "export_adjacency: written " << cur_elements << " nodes to " << outpath << std::endl;
}


// If your local hnswlib has internals private, apply the suggested header patch so that the following members are accessible:
// enterpoint_node_, maxlevel_, element_levels_, linkLists_, size_links_per_element_, data_level0_memory_, size_data_per_element_
int main(int argc, char** argv) {
    size_t N = 100000;
    size_t dim = 128;
    int M = 16; // max neighbors
    int ef_construction = 200;
    std::string dbpath = "./rocksdb_data";
    std::string graph_out = "./hnsw_graph.bin";
    if (argc>1) N = std::stoul(argv[1]);
    if (argc>2) dim = std::stoul(argv[2]);
    if (argc>3) dbpath = argv[3];
    if (argc>4) graph_out = argv[4];
    if (argc>5) M = std::stoi(argv[5]);
    if (argc>6) ef_construction = std::stoi(argv[6]);

    std::mt19937_64 rng(123);
    std::normal_distribution<float> nd(0.0f,1.0f);

    rocksdb::Options options;
    options.create_if_missing=true;
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, dbpath, &db);
    if (!s.ok()) { std::cerr<<"RocksDB open error: "<<s.ToString()<<"\n"; return 1; }

    // build hnsw
    hnswlib::L2Space l2space((int)dim);
    // hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N);
    hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N, M, ef_construction);

    std::vector<float> v(dim);
    for (size_t i=0;i<N;i++){
        for (size_t d=0; d<dim; d++) v[d] = nd(rng);
        uint32_t id = (uint32_t)i;
        // store to rocksdb
        std::string key(reinterpret_cast<const char*>(&id), sizeof(id));
        std::string val = vec_to_bytes(std::vector<float>(v.begin(), v.end()));
        db->Put(rocksdb::WriteOptions(), key, val);
        appr_alg.addPoint((void*)v.data(), id);
        if ((i+1)%10000==0) std::cerr<<"added "<<(i+1)<<" points\n";
    }

    appr_alg.saveIndex(graph_out);
    std::cerr << "HNSW index saved to " << graph_out << std::endl;

    export_adjacency(appr_alg, graph_out + ".adj");

    delete db;
    return 0;

    // // 导出邻接表
    // std::string adj_out = graph_out + ".adj";
    // std::ofstream out(adj_out, std::ios::binary);
    // if (!out) { std::cerr<<"Cannot write adjacency file "<<adj_out<<"\n"; delete db; return 1; }

    // // 写入 header（entrypoint, max_level, node_count）都作为 uint32_t（小端）
    // uint32_t node_count = static_cast<uint32_t>(appr_alg.cur_element_count);
    // uint32_t entrypoint = static_cast<uint32_t>(appr_alg.enterpoint_node_);
    // uint32_t max_level = static_cast<uint32_t>(appr_alg.maxlevel_);
    // std::cout << "Exporting adjacency: nodes=" << node_count 
    //           << ", entry=" << entrypoint << ", max_level=" << max_level << std::endl;
    // out.write(reinterpret_cast<const char*>(&entrypoint), sizeof(entrypoint));
    // out.write(reinterpret_cast<const char*>(&max_level), sizeof(max_level));
    // out.write(reinterpret_cast<const char*>(&node_count), sizeof(node_count));
    // std::cout << "Header written to adjacency file" << std::endl;

    // if (appr_alg.element_levels_.empty() || !appr_alg.linkLists_) 
    // {
    //     std::cerr << "Warning: HNSW internal data structures not accessible" << std::endl;
    //     std::cerr << "element_levels_: " << (!appr_alg.element_levels_.empty() ? "valid" : "null") << std::endl;
    //     std::cerr << "linkLists_: " << (appr_alg.linkLists_ ? "valid" : "null") << std::endl;

    //     for (uint32_t internal_idx = 0; internal_idx < node_count; ++internal_idx) {
    //         uint32_t label = static_cast<uint32_t>(appr_alg.getExternalLabel(internal_idx));
    //         out.write(reinterpret_cast<const char*>(&label), sizeof(label));
    //         uint32_t levels_u = 0;
    //         out.write(reinterpret_cast<const char*>(&levels_u), sizeof(levels_u));
    //     }
        
    //     delete db;
    //     return 0;
    // }
    // std::cout << "Exporting adjacency data..." << std::endl;

    // uint64_t expected_linklists_elems = (uint64_t)(appr_alg.maxlevel_ + 1) * (uint64_t)node_count;
    // std::cerr << "Debug: expected_linklists_elems=" << expected_linklists_elems << std::endl;

    // // 对每个 internal index 导出 label 和每层邻居（只导出 level0 也可，但这里按通用格式导出所有层以保留信息）
    // for (uint32_t internal_idx = 0; internal_idx < node_count; ++internal_idx) {
    //     std::cout << "Exporting internal_idx=" << internal_idx << std::endl;
    //     uint32_t label = static_cast<uint32_t>(appr_alg.getExternalLabel(internal_idx));
    //     std::cout << "  label=" << label << std::endl;
    //     out.write(reinterpret_cast<const char*>(&label), sizeof(label));

    //     if (internal_idx >= appr_alg.element_levels_.size()) {
    //     std::cerr << "Warning: element_levels_ shorter than cur_element_count\n";
    //     uint32_t zero = 0;
    //     out.write(reinterpret_cast<const char*>(&zero), sizeof(zero));
    //     continue;
    // }

    //     // int levels = appr_alg.element_levels_[internal_idx];
    //     int levels = 0;
    //     try
    //     {
    //         levels = appr_alg.element_levels_[internal_idx];
    //     }
    //     catch(...)
    //     {
    //         std::cerr << "Error: Unable to access element_levels_" << std::endl;
    //     }
        
    //     if (levels <= 0 || levels > appr_alg.maxlevel_ + 1) 
    //     {
    //         std::cerr << "Warning: invalid level count " << levels 
    //                 << " at node " << internal_idx << std::endl;
    //         levels = 1;
    //     }

    //     uint32_t levels_u = static_cast<uint32_t>(levels);
    //     out.write(reinterpret_cast<const char*>(&levels_u), sizeof(levels_u));

    //     for (int lev = 0; lev < levels; ++lev) {
    //         // linkLists_ 存储的是 char* 指向一个内存块，第一项通常是 degree，然后是 degree 个 uint32_t neighbor



    //         uint64_t idx = (uint64_t)internal_idx * (uint64_t)(appr_alg.maxlevel_ + 1) + (uint64_t)lev;
    //         if (idx >= expected_linklists_elems) {
    //             // 索引超出预期范围，写 0 并继续，打印警告
    //             uint32_t deg0 = 0;
    //             out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
    //             std::cerr << "Warning: computed linkLists_ index out of bound: internal_idx=" 
    //                       << internal_idx << " lev=" << lev << " idx=" << idx << "\n";
    //             continue;
    //         }

    //         char* raw = nullptr;
    //         try
    //         {
    //             raw = appr_alg.linkLists_[idx];
    //         }
    //         catch(...)
    //         {
    //             std::cerr << "Error: Unable to access linkLists_" << std::endl;
    //         }
            

    //         // size_t idx = internal_idx * (appr_alg.maxlevel_ + 1) + lev;

    //         // 安全检查：确保索引在有效范围内
    //         if (idx >= (appr_alg.maxlevel_ + 1) * node_count) {
    //             std::cerr << "Error: Invalid linkList index " << idx << std::endl;
    //             uint32_t deg0 = 0;
    //             out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
    //             continue;
    //         }

    //         raw = appr_alg.linkLists_[ idx ];

    //         if (!raw) {
    //             uint32_t deg0 = 0;
    //             out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
    //             continue;
    //         }

    //         // read degree safely (some versions use int32 or uint16 for degree)
    //         int32_t deg32 = 0;
    //         std::memcpy(&deg32, raw, sizeof(deg32));

    //         if (deg32 >= 0 && deg32 < node_count) {
    //             uint32_t deg = static_cast<uint32_t>(deg32);
    //             if (deg > node_count) {
    //                 std::cerr << "Warning: deg > node_count for internal_idx="<<internal_idx<<", lev="<<lev<<", deg="<<deg<<"\n";
    //                 deg = 0;
    //             }
    //             out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));
    //             size_t offset = sizeof(deg32);
    //             for (uint32_t j = 0; j < deg; ++j) {
    //                 uint32_t nb;
    //                 std::memcpy(&nb, raw + offset + j * sizeof(nb), sizeof(nb));
    //                 out.write(reinterpret_cast<const char*>(&nb), sizeof(nb));
    //             }
    //         } else {
    //             // fallback try uint16_t degree
    //             uint16_t deg16 = 0;
    //             std::memcpy(&deg16, raw, sizeof(deg16));
    //             if(deg16 == 0 || deg16 > node_count)
    //             {
    //                 std::cerr << "Error: Unreasonable degree value " << deg32 << " or " << deg16 
    //                           << " at internal_idx=" << internal_idx << " level=" << lev << std::endl;
    //                 uint32_t deg0 = 0;
    //                 out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
    //                 continue;
    //             }
    //             uint32_t deg = static_cast<uint32_t>(deg16);
    //             out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));
    //             size_t offset = sizeof(deg16);
    //             for (uint32_t j = 0; j < deg; ++j) {
    //                 uint32_t nb;
    //                 std::memcpy(&nb, raw + offset + j * sizeof(nb), sizeof(nb));
    //                 out.write(reinterpret_cast<const char*>(&nb), sizeof(nb));
    //             }
    //         }
    //     }
    // }

    
}
