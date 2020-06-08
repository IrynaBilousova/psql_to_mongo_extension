#ifndef _psql_to_mongo_c_to_cpp_h_
#define _psql_to_mongo_c_to_cpp_h_

#ifdef __cplusplus
extern "C"{
#endif 

void psql_mongo_replication_cpp_connect_mongo_db(
      const char* dbname
    , const char* host
    , const char* port
    , const char* username
    , const char* password
    , unsigned int id);

void psql_mongo_replication_cpp_start_replication(
      const char* dbname
    , const char* host
    , const char* port
    , const char* username
    , const char* password);

void psql_mongo_replication_cpp_reconnect_mongo_db(int id);

void psql_mongo_replication_cpp_test_linking();

#ifdef __cplusplus
}
#endif

#endif