#include "hnsw_graph.h"
#include "../httplib.h"
#include <../nlohmann/json.hpp>
#include <fstream>

using json = nlohmann::json;

size_t get_current_rss() {
    std::ifstream statm("/proc/self/statm");
    long rss_pages = 0;
    statm >> rss_pages >> rss_pages;
    long page_size_kb = sysconf(_SC_PAGESIZE) / 1024;
    return rss_pages * page_size_kb; // KB
}

int main(int argc, char** argv) {
    std::string graph_file = "./hnsw_graph.bin";
    std::string storage_host = "http://127.0.0.1:8081";
    int port = 8080;
    size_t ef = 200;
    size_t k_default = 10;
    uint32_t entry = 0;
    bool optimized = false;

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
    }

    HNSWGraph g;
    if (!g.load_from_file(graph_file, optimized)) return 1;

    if (entry == 0 && g.id_to_index.size()>0) {
        
        entry = g.entrypoint;
    }

    httplib::Server svr;
    svr.Post("/search", [&](const httplib::Request& req, httplib::Response& res){
        try {
            json j = json::parse(req.body);
            std::vector<float> query = j["query"].get<std::vector<float>>();
            size_t k = j.value("k", (int)k_default);
            size_t efq = j.value("ef", (int)ef);
            uint32_t entry_id = j.value("entry_id", (int)entry);

            auto out = g.search_candidates(g, storage_host, query, entry_id, efq, k);
            json resp = json::object();
            resp["results"] = json::array();
            for (auto &p: out){
                resp["results"].push_back( json::object( { {"id", p.first}, {"distance", p.second} } ) );
            }
            res.set_content(resp.dump(), "application/json");
        } catch (const std::exception &e) {
            res.status = 500;
            res.set_content(std::string("error: ") + e.what(), "text/plain");
        }
    });

    svr.Get("/info", [&](const httplib::Request&, httplib::Response& res){
        json info;
        info["nodes"] = g.adjacency.size();
        info["storage"] = storage_host;
        info["ef"] = ef;
        res.set_content(info.dump(), "application/json");
    });

    svr.Get("/mem", [&](const httplib::Request&, httplib::Response& res){
    size_t rss_kb = get_current_rss();
    json j;
    j["rss_kb"] = rss_kb;
    res.set_content(j.dump(), "application/json");
    });


    std::cout<<"hnsw_service listening on port "<<port<<"\n";
    svr.listen("0.0.0.0", port);
    return 0;
}