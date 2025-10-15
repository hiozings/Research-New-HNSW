#include "rocksdb_store.h"
#include "../tools/common.h"
#include "../httplib.h" 
#include <nlohmann/json.hpp>
#include <iostream>

using json = nlohmann::json;

int main(int argc, char** argv)
{
    std::string dbpath = "./rocksdb_data";
    int port = 8081;
    if (argc > 1) dbpath = argv[1];
    if (argc > 2) port = atoi(argv[2]);

    RocksDBStore store(dbpath);
    httplib::Server svr;

    //存储
    svr.Post(R"(/vec/put)", [&](const httplib::Request& req, httplib::Response& res){
        // 请求体格式：前4字节ID + 4字节维度 + 浮点数数组
        
        const std::string& b = req.body;
        if (b.size() < sizeof(uint32_t)*2) { res.status = 400; return; }

        uint32_t id; 
        uint32_t dim;
        memcpy(&id, b.data(), 4);
        memcpy(&dim, b.data() + 4,4);

        size_t expected = dim * sizeof(float);
        if (b.size() != 8 + expected) { res.status = 400; return; }
        std::vector<float> v(dim);
        memcpy(v.data(), b.data() + 8, expected);

        bool ok = store.put_vector(id, v);
        res.set_content(ok?"OK":"ERR", "text/plain");
    });

    //查询
    svr.Get(R"(/vec/get)", [&](const httplib::Request& req, httplib::Response& res){
        // query: ?id=123
        auto q = req.get_param_value("id");
        uint32_t id = std::stoul(q);

        std::vector<float> v;
        if (!store.get_vector(id, v)) { res.status = 404; return; }

        json j; j["id"] = id; j["values"] = v;
        res.set_content(j.dump(), "application/json");
    });

    //批量查询
    svr.Post(R"(/vec/batch_get)", [&](const httplib::Request& req, httplib::Response& res){
        //解析JSON格式的ID数组
        try {
            auto j = json::parse(req.body);
            std::vector<uint32_t> ids = j.get<std::vector<uint32_t>>();

            std::vector<std::vector<float>> vecs;
            store.batch_get_vectors(ids, vecs);

            // 返回格式：每个向量前加4字节长度标识
            json out = json::array();
            for (size_t i=0;i<ids.size();++i){
                if (vecs[i].empty()) out.push_back(nullptr);
                else out.push_back(vecs[i]);
            }
            res.set_content(out.dump(), "application/json");

        } catch (...) { res.status = 400; res.set_content("bad json","text/plain");}
    });

    std::cout << "Starting storage_service on port "<<port<<" with db "<<dbpath<<"\n";
    svr.listen("0.0.0.0", port);
    return 0;
}