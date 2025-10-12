// common.h - shared utilities
#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>

using vid_t = uint32_t;
using dim_t = uint32_t;

struct VecHeader {
vid_t id;
dim_t dim;
};
// 浮点数向量转化为字符串
inline std::string vec_to_bytes(const std::vector<float>& v) 
{
    std::string s;
    s.resize(v.size()*sizeof(float));
    memcpy(s.data(), v.data(), s.size());
    return s;
}
//字符串转化为浮点数向量
inline std::vector<float> bytes_to_vec(const std::string& s) 
{
    size_t n = s.size()/sizeof(float);
    std::vector<float> v(n);
    memcpy(v.data(), s.data(), s.size());
    return v;
}