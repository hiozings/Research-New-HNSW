#pragma once
#include <rocksdb/db.h>
#include <string>
#include <vector>
#include <memory>

class RocksDBStore 
{
    public:
        RocksDBStore(const std::string& db_path);
        ~RocksDBStore();
        bool put_vector(uint32_t id, const std::vector<float>& vec);
        bool get_vector(uint32_t id, std::vector<float>& out);
        bool batch_get_vectors(const std::vector<uint32_t>& ids, std::vector<std::vector<float>>& out);

    private:
        rocksdb::DB* db_ = nullptr;
};
