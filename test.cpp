#include "hnswlib/hnswlib.h"

int main()
{
    size_t N = 10000;
    size_t dim = 128;
    int M = 16; // max neighbors
    int ef_construction = 200;
    hnswlib::L2Space l2space((int)dim);
    // hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N);
    hnswlib::HierarchicalNSW<float> appr_alg(&l2space, N, M, ef_construction);
    std::mt19937_64 rng(123);
    std::normal_distribution<float> nd(0.0f,1.0f);
    std::vector<float> v(dim);
    for (size_t i=0;i<N;i++){
        for (size_t d=0; d<dim; d++) v[d] = nd(rng);
        uint32_t id = (uint32_t)i;
        appr_alg.addPoint((void*)v.data(), id);
        if ((i+1)%10000==0) std::cerr<<"added "<<(i+1)<<" points\n";
    }
    uint32_t node_count = static_cast<uint32_t>(appr_alg.cur_element_count);
    uint32_t entrypoint = static_cast<uint32_t>(appr_alg.enterpoint_node_);
    uint32_t max_level = static_cast<uint32_t>(appr_alg.maxlevel_);
    std::cout << "Exporting adjacency: nodes=" << node_count 
              << ", entry=" << entrypoint << ", max_level=" << max_level << std::endl;
    return 0;
}
