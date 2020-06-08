cmake_minimum_required (VERSION 3.15.0)

include_directories("/usr/include/libmongoc-1.0")
include_directories("/usr/include/libbson-1.0")

add_library (psql_mongo_replication_lib STATIC)


target_sources(psql_mongo_replication_lib
    PUBLIC
    include/psql_mongo_replication/psql_mongo_replication.hpp
    include/psql_mongo_replication/psql_to_mongo_c_to_cpp_call_api.h
    PRIVATE
    src/psql_mongo_replication/psql_to_mongo_c_to_cpp_call_api.cpp
    src/psql_mongo_replication/mongo_replication.hpp
    src/psql_mongo_replication/psql_mongo_replication.cpp
    src/psql_mongo_replication/mongo_replication.cpp
)

target_include_directories(psql_mongo_replication_lib PRIVATE ./src)

target_include_directories(psql_mongo_replication_lib PUBLIC  ./include)

target_link_libraries(psql_mongo_replication_lib 
    /usr/lib/x86_64-linux-gnu/libmongoc-1.0.so.0 
    /usr/lib/x86_64-linux-gnu/libbson-1.0.so.0
    rapidjson
    psql_mongo_replication::pg_recvlogical
    pthread)

add_executable (psql_mongo_replication
    src/stdafx.hpp
    src/main.cpp)

target_compile_definitions(psql_mongo_replication PUBLIC CMD_BUILD)

target_link_libraries(psql_mongo_replication PRIVATE psql_mongo_replication_lib)

add_dependencies(
    psql_mongo_replication_lib 
    psql_mongo_replication::pg_recvlogical)
