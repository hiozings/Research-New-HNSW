#include <iostream>
#include <vector>
#include <random>
#include <fstream>
#include <string>
#include <rocksdb/db.h>
#include "../tools/common.h"

// hnswlib includes (assume third_party/hnswlib contains headers)
#include "../hnswlib/hnswlib.h"

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

    std::string adj_out = graph_out + ".adj";
    std::ofstream out(adj_out, std::ios::binary);
    if (!out) { std::cerr<<"Cannot write adjacency file "<<adj_out<<"\n"; delete db; return 1; }

    // 写入 header（entrypoint, max_level, node_count）都作为 uint32_t（小端）
    uint32_t node_count = static_cast<uint32_t>(appr_alg.cur_element_count);
    uint32_t entrypoint = static_cast<uint32_t>(appr_alg.enterpoint_node_);
    uint32_t max_level = static_cast<uint32_t>(appr_alg.maxlevel_);
    out.write(reinterpret_cast<const char*>(&entrypoint), sizeof(entrypoint));
    out.write(reinterpret_cast<const char*>(&max_level), sizeof(max_level));
    out.write(reinterpret_cast<const char*>(&node_count), sizeof(node_count));

    // 对每个 internal index 导出 label 和每层邻居（只导出 level0 也可，但这里按通用格式导出所有层以保留信息）
    for (uint32_t internal_idx = 0; internal_idx < node_count; ++internal_idx) {
        uint32_t label = static_cast<uint32_t>(appr_alg.getExternalLabel(internal_idx));
        out.write(reinterpret_cast<const char*>(&label), sizeof(label));

        int levels = appr_alg.element_levels_[internal_idx];
        uint32_t levels_u = static_cast<uint32_t>(levels);
        out.write(reinterpret_cast<const char*>(&levels_u), sizeof(levels_u));

        for (int lev = 0; lev < levels; ++lev) {
            // linkLists_ 存储的是 char* 指向一个内存块，第一项通常是 degree，然后是 degree 个 uint32_t neighbor
            char* raw = nullptr;
            size_t idx = internal_idx * (appr_alg.maxlevel_ + 1) + lev;
            raw = appr_alg.linkLists_[ idx ];

            if (!raw) {
                uint32_t deg0 = 0;
                out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
                continue;
            }

            // read degree safely (some versions use int32 or uint16 for degree)
            int32_t deg32 = 0;
            std::memcpy(&deg32, raw, sizeof(deg32));

            if (deg32 >= 0 && deg32 < 10000000) {
                uint32_t deg = static_cast<uint32_t>(deg32);
                out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));
                size_t offset = sizeof(deg32);
                for (uint32_t j = 0; j < deg; ++j) {
                    uint32_t nb;
                    std::memcpy(&nb, raw + offset + j * sizeof(nb), sizeof(nb));
                    out.write(reinterpret_cast<const char*>(&nb), sizeof(nb));
                }
            } else {
                // fallback try uint16_t degree
                uint16_t deg16 = 0;
                std::memcpy(&deg16, raw, sizeof(deg16));
                uint32_t deg = static_cast<uint32_t>(deg16);
                out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));
                size_t offset = sizeof(deg16);
                for (uint32_t j = 0; j < deg; ++j) {
                    uint32_t nb;
                    std::memcpy(&nb, raw + offset + j * sizeof(nb), sizeof(nb));
                    out.write(reinterpret_cast<const char*>(&nb), sizeof(nb));
                }
            }
        }
    }

    delete db;
    return 0;
}
