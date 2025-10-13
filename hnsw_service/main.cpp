#include "hnsw_graph.h"
#include "../httplib.h"
#include <../nlohmann/json.hpp>
#include <fstream>
#include "../hnswlib/hnswlib.h"


using json = nlohmann::json;

// size_t get_current_rss() {
//     std::ifstream statm("/proc/self/statm");
//     long rss_pages = 0;
//     statm >> rss_pages >> rss_pages;
//     long page_size_kb = sysconf(_SC_PAGESIZE) / 1024;
//     return rss_pages * page_size_kb; // KB
// }
size_t get_current_rss_kb() {
    std::ifstream statm("/proc/self/statm");
    long total_pages = 0, rss_pages = 0;
    statm >> total_pages >> rss_pages;
    long page_size_kb = sysconf(_SC_PAGESIZE) / 1024;
    return rss_pages * page_size_kb;
}

int main(int argc, char** argv) {
    std::string graph_file = "./hnsw_graph.bin";
    std::string storage_host = "http://127.0.0.1:8081";
    int port = 8080;
    size_t ef = 200;
    size_t k_default = 10;
    uint32_t entry = 0;
    bool optimized = false;
    int dim = 128;

    for (int i=1;i<argc;i++){
        std::string a = argv[i];
        if (a=="--graph" && i+1<argc) graph_file = argv[++i];
        else if (a=="--storage" && i+1<argc) storage_host = argv[++i];
        else if (a=="--port" && i+1<argc) port = atoi(argv[++i]);
        else if (a=="--ef" && i+1<argc) ef = std::stoul(argv[++i]);
        else if (a=="--k" && i+1<argc) k_default = std::stoul(argv[++i]);
        else if (a=="--entry" && i+1<argc) entry = (uint32_t)std::stoul(argv[++i]);
        else if (a=="--optimized" && i+1<argc) {
            std::string val = argv[++i];
            optimized = (val == "1" || val == "true" || val == "True");
        }
        else if (a=="--dim" && i+1<argc) dim = atoi(argv[++i]);
    }

    // HNSWGraph g;
    // if (!g.load_from_file(graph_file, optimized)) return 1;

    // if (entry == 0 && g.id_to_index.size()>0) {
        
    //     entry = g.entrypoint;
    // }
    httplib::Server svr;
    std::unique_ptr<hnswlib::L2Space> l2space;
    std::unique_ptr<hnswlib::HierarchicalNSW<float>> hnsw;
    if (!optimized) 
    {
        std::cout << "[mode] normal (in-memory)\n";
        l2space = std::make_unique<hnswlib::L2Space>(dim);
        hnsw = std::make_unique<hnswlib::HierarchicalNSW<float>>(l2space.get(), graph_file);
        std::cout << "Loaded HNSW graph: " << hnsw->cur_element_count << " nodes\n";


        svr.Post("/search", [&](const httplib::Request& req, httplib::Response& res){
            try {
                json j = json::parse(req.body);
                std::vector<float> query = j["query"].get<std::vector<float>>();
                int k = j.value("k", k_default);
                int efq = j.value("ef", ef);

                hnsw->setEf(efq);
                auto result = hnsw->searchKnn(query.data(), k);

                json resp;
                resp["results"] = json::array();

                while (!result.empty()) {
                    auto [dist, id] = result.top();
                    result.pop();
                    resp["results"].push_back({{"id", id}, {"distance", dist}});
                }
                // std::vector<std::pair<float, hnswlib::labeltype>> temp;
                // while (!result.empty()) 
                // {
                //     temp.push_back(result.top());
                //     result.pop();
                // }
                // // 反转顺序，让最近的结果在前
                // for (auto it = temp.rbegin(); it != temp.rend(); ++it) 
                // {
                //     resp["results"].push_back({{"id", it->second}, {"distance", it->first}});
                // }

                resp["rss_kb"] = get_current_rss_kb(); // 实时内存占用
                res.set_content(resp.dump(), "application/json");
            } catch (const std::exception &e) {
                res.status = 500;
                res.set_content(std::string("error: ") + e.what(), "text/plain");
            }
        });

        svr.Get("/info", [&](const httplib::Request&, httplib::Response& res){
            json info;
            uint64_t nodes = static_cast<uint64_t>(hnsw->cur_element_count.load());
            info["nodes"] = nodes;
            info["dim"] = dim;
            info["ef"] = ef;
            res.set_content(info.dump(), "application/json");
        });

        

    }
    else
    {
        std::cout << "[mode] optimized (storage-compute separation)\n";

        std::string adj_path = graph_file + ".adj";

        // ✅ 使用智能指针管理生命周期
        auto g_ptr = std::make_shared<HNSWGraph>();

        if (!g_ptr->load_from_file(adj_path, true)) {
            std::cerr << "Failed to load adjacency file: " << adj_path << "\n";
            return 1;
        }

        std::cout << "Loaded adjacency-only graph: nodes=" << g_ptr->adjacency.size()
                << ", entry=" << g_ptr->entrypoint << "\n";

        svr.Post("/search", [g_ptr, storage_host, k_default, ef](const httplib::Request& req, httplib::Response& res) {
            try {
                json j = json::parse(req.body);
                std::vector<float> query = j["query"].get<std::vector<float>>();
                int k = j.value("k", (int)k_default);
                int efq = j.value("ef", (int)ef);
                uint32_t entry_id = j.value("entry_id", (int)g_ptr->entrypoint);

                auto out = g_ptr->search_candidates(*g_ptr, storage_host, query, entry_id, efq, k);

                json resp;
                resp["results"] = json::array();
                for (auto &p : out) {
                    resp["results"].push_back({{"id", p.first}, {"distance", p.second}});
                }
                resp["rss_kb"] = get_current_rss_kb();
                resp["mode"] = "optimized";
                res.set_content(resp.dump(), "application/json");
            } catch (const std::exception &e) {
                res.status = 500;
                res.set_content(std::string("error: ") + e.what(), "text/plain");
            }
        });

        svr.Get("/info", [g_ptr, dim, ef, storage_host](const httplib::Request&, httplib::Response& res) {
            json info;
            info["nodes"] = g_ptr->adjacency.size();
            info["dim"] = dim;
            info["ef"] = ef;
            info["storage"] = storage_host;
            info["mode"] = "optimized";
            res.set_content(info.dump(), "application/json");
        });
    }

    svr.Get("/mem", [&](const httplib::Request&, httplib::Response& res){
            json j;
            j["rss_kb"] = get_current_rss_kb();
            res.set_content(j.dump(), "application/json");
        });

    std::cout<<"hnsw_service listening on port "<<port<<"\n";
    svr.listen("0.0.0.0", port);
    return 0;
}