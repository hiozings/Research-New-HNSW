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
    std::string dbpath = "./rocksdb_data";
    std::string graph_out = "./hnsw_graph.bin";
    if (argc>1) N = std::stoul(argv[1]);
    if (argc>2) dim = std::stoul(argv[2]);
    if (argc>3) dbpath = argv[3];
    if (argc>4) graph_out = argv[4];

    std::mt19937_64 rng(123);
    std::normal_distribution<float> nd(0.0f,1.0f);

    rocksdb::Options options;
    options.create_if_missing=true;
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, dbpath, &db);
    if (!s.ok()) { std::cerr<<"RocksDB open error: "<<s.ToString()<<"\n"; return 1; }

    // build hnsw
    hnswlib::L2Space l2space((int)dim);
    hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N);

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


    delete db;
    return 0;
}
