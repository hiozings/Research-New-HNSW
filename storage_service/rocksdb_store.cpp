#include "rocksdb_store.h"
#include "../tools/common.h"
#include <cassert>
#include <sstream>

RocksDBStore::RocksDBStore(const std::string& db_path)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
    if (!s.ok())
    {
        throw std::runtime_error("Failed to open RocksDB: " + s.ToString());
    }
}

RocksDBStore::~RocksDBStore() 
{
    delete db_;
}

//存储
bool RocksDBStore::put_vector(uint32_t id, const std::vector<float>& vec)
{
    std::string key(reinterpret_cast<const char*>(&id), sizeof(id));
    std::string val = vec_to_bytes(vec);
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, val);
    return s.ok();
}

//读取
bool RocksDBStore::get_vector(uint32_t id, std::vector<float>& out)
{
    std::string key(reinterpret_cast<const char*>(&id), sizeof(id));
    std::string val;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &val);
    if (!s.ok()) return false;
    out = bytes_to_vec(val);
    return true;
}

//批量读取
bool RocksDBStore::batch_get_vectors(const std::vector<uint32_t>& ids, std::vector<std::vector<float>>& out)
{
    out.clear();
    out.reserve(ids.size());
    rocksdb::ReadOptions ro;
    for (auto id: ids) 
    {
        std::string key(reinterpret_cast<const char*>(&id), sizeof(id));
        std::string val;
        rocksdb::Status s = db_->Get(ro, key, &val);
        if (!s.ok()) { out.emplace_back(); continue; }
        out.push_back(bytes_to_vec(val));
    }
    return true;
}
