# cmake_minimum_required(VERSION 3.12)
# project(hnsw_projects)

# set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_BUILD_TYPE Release)

# # === include directories ===
# include_directories(
#     ${PROJECT_SOURCE_DIR}/hnswlib
#     ${PROJECT_SOURCE_DIR}/nlohmann
#     ${PROJECT_SOURCE_DIR}/tools
#     ${PROJECT_SOURCE_DIR}              # for httplib.h
#     /usr/local/include
# )

# # === find RocksDB (system installed) ===
# include_directories(/usr/local/include)
# link_directories(/usr/local/lib)

# # if(NOT RocksDB_FOUND)
# #     include_directories(${ROCKSDB_INCLUDE_DIRS})
# #     add_library(RocksDB::RocksDB STATIC IMPORTED)
# #     set_target_properties(RocksDB::RocksDB PROPERTIES IMPORTED_LOCATION ${ROCKSDB_LIBRARIES})
# # endif()


# # === link dependencies ===
# set(COMMON_LIBS
#     ${RocksDB_LIBRARIES}
#     pthread
#     snappy
#     z
#     bz2
#     lz4
#     zstd
# )

# # === storage_service target ===
# add_executable(storage_service
#     storage_service/main.cpp
#     storage_service/rocksdb_store.cpp
# )
# target_link_libraries(storage_service ${COMMON_LIBS})

# # === hnsw_service target ===
# add_executable(hnsw_service
#     hnsw_service/main.cpp
#     hnsw_service/hnsw_graph.cpp
# )
# target_link_libraries(hnsw_service ${COMMON_LIBS})

# # === index_builder target ===
# add_executable(index_builder
#     index_builder/build.cpp
# )
# target_link_libraries(index_builder ${COMMON_LIBS})

# # === optional install targets ===
# install(TARGETS storage_service hnsw_service index_builder DESTINATION bin)

cmake_minimum_required(VERSION 3.14)
project(HNSW_Vector_Store)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 添加 include 路径
include_directories(
    ${PROJECT_SOURCE_DIR}             # for httplib.h
    ${PROJECT_SOURCE_DIR}/hnswlib
    ${PROJECT_SOURCE_DIR}/nlohmann
    ${PROJECT_SOURCE_DIR}/tools
    /usr/local/include
)

# 指定 RocksDB 库路径
link_directories(/usr/local/lib)


find_package(Threads REQUIRED)

# RocksDB 静态库路径
set(ROCKSDB_LIB /usr/local/lib/librocksdb.a)

# 链接的系统库
set(COMMON_LIBS
    ${ROCKSDB_LIB}
    Threads::Threads
    z bz2 zstd snappy lz4 dl m stdc++
)

# ----------------------------
# storage_service
# ----------------------------
add_executable(storage_service
    storage_service/main.cpp
    storage_service/rocksdb_store.cpp
)

target_link_libraries(storage_service
    ${COMMON_LIBS}
)

# ----------------------------
# hnsw_service
# ----------------------------
add_executable(hnsw_service
    hnsw_service/main.cpp
    hnsw_service/hnsw_graph.cpp
)

target_link_libraries(hnsw_service
    ${COMMON_LIBS}
)

# ----------------------------
# index_builder
# ----------------------------
add_executable(index_builder
    index_builder/build.cpp
)

target_link_libraries(index_builder
    ${COMMON_LIBS}
)

set(TARGET_OUTPUT_DIR "$ENV{HOME}/projects/pypro/hnsw")

set_target_properties(storage_service PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TARGET_OUTPUT_DIR}/bin)
set_target_properties(hnsw_service PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TARGET_OUTPUT_DIR}/bin)
set_target_properties(index_builder PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${TARGET_OUTPUT_DIR}/bin)