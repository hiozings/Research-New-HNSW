#include <iostream>
#include <vector>
#include <random>
#include <fstream>
#include <string>
#include <rocksdb/db.h>
#include "../tools/common.h"
#include "../hnswlib/hnswlib.h"


using tableint = unsigned int;
using linklistsizeint = unsigned int;

// 导出邻接表到二进制文件
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

    if (appr_alg.element_levels_.size() < cur_elements) {
        throw std::runtime_error("export_adjacency: element_levels_ too small");
    }

    // 读取 entrypoint 和 maxlevel
    int enterpoint = appr_alg.enterpoint_node_;
    int maxlevel = appr_alg.maxlevel_;

    std::ofstream out(outpath, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open adjacency output file");

    uint32_t entry_u = static_cast<uint32_t>(enterpoint < 0 ? 0 : enterpoint);
    uint32_t maxlevel_u = static_cast<uint32_t>(maxlevel < 0 ? 0 : maxlevel);
    uint32_t node_count_u = static_cast<uint32_t>(cur_elements);

    out.write(reinterpret_cast<const char*>(&entry_u), sizeof(entry_u));
    out.write(reinterpret_cast<const char*>(&maxlevel_u), sizeof(maxlevel_u));
    out.write(reinterpret_cast<const char*>(&node_count_u), sizeof(node_count_u));

    for (size_t internal_idx = 0; internal_idx < cur_elements; ++internal_idx) {
       
        unsigned int lbl = static_cast<unsigned int>(appr_alg.getExternalLabel(static_cast<tableint>(internal_idx)));
        out.write(reinterpret_cast<const char*>(&lbl), sizeof(lbl));

       
        int lev = appr_alg.element_levels_[internal_idx];
        if (lev < 0) lev = 0;
        uint32_t levels_u = static_cast<uint32_t>(lev + 1);
        out.write(reinterpret_cast<const char*>(&levels_u), sizeof(levels_u));

       
        for (int level = 0; level <= lev; ++level) {
            
            linklistsizeint *ll = nullptr;
            try {
                ll = appr_alg.get_linklist_at_level(static_cast<tableint>(internal_idx), level);
            } catch (...) {
                uint32_t deg0 = 0;
                out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
                continue;
            }
           
            unsigned short deg_us = appr_alg.getListCount(ll);
            uint32_t deg = static_cast<uint32_t>(deg_us);

          
            const uint32_t MAX_REASONABLE_DEG = 1000000;
            if (deg > MAX_REASONABLE_DEG) {
                std::cerr << "export_adjacency: unreasonable deg " << deg
                          << " at internal_idx=" << internal_idx << " level=" << level << "\n";
                uint32_t deg0 = 0;
                out.write(reinterpret_cast<const char*>(&deg0), sizeof(deg0));
                continue;
            }

            out.write(reinterpret_cast<const char*>(&deg), sizeof(deg));

            if (deg == 0) continue;

            tableint *neighbors_ptr = reinterpret_cast<tableint*>(ll + 1);

            for (uint32_t j = 0; j < deg; ++j) {
                tableint nb_internal = neighbors_ptr[j];
                if (nb_internal < 0 || static_cast<size_t>(nb_internal) >= cur_elements) {
                    unsigned int nb_label = 0; // invalid -> write 0
                    out.write(reinterpret_cast<const char*>(&nb_label), sizeof(nb_label));
                } else {
                    uint32_t nb_internal_idx = static_cast<uint32_t>(nb_internal);
                    out.write(reinterpret_cast<const char*>(&nb_internal_idx), sizeof(nb_internal_idx));
                }
            }
        }

    }

    out.close();
    std::cerr << "export_adjacency: written " << cur_elements << " nodes to " << outpath << std::endl;
}


int main(int argc, char** argv) {
    size_t N = 100000;
    size_t dim = 128;
    int M = 16; // 最大邻居数
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

    hnswlib::L2Space l2space((int)dim);
    hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N, M, ef_construction);

    std::vector<float> v(dim);
    for (size_t i=0;i<N;i++){
        for (size_t d=0; d<dim; d++) v[d] = nd(rng);
        uint32_t id = (uint32_t)i;
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
}
