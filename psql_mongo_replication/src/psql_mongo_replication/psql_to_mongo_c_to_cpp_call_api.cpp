#include "psql_mongo_replication/psql_mongo_replication.hpp"
#include "pg_recvlogical/pg_recvlogical.h"
#include "psql_mongo_replication/psql_to_mongo_c_to_cpp_call_api.h"

psql_mongo_replication::psql_to_mongo psqlToMongo;

void psql_mongo_replication_cpp_connect_mongo_db(
      const char* dbname
    , const char* host
    , const char* port
    , const char* username
    , const char* password
    , unsigned int id)
{
    pg_recvlogical_connection_settings_t connection;

    connection._dbname = dbname;
    connection._port = port;
    connection._host = host;
    connection._password = password;
    connection._username = username;
    connection._id = id;

    psqlToMongo.connect_to_mongo_db(connection);
}

void psql_mongo_replication_cpp_start_replication(
      const char* dbname
    , const char* host
    , const char* port
    , const char* username
    , const char* password)
{
    pg_recvlogical_connection_settings_t host_connection;

    host_connection._dbname = dbname;
    host_connection._port = port;
    host_connection._host = host;
    host_connection._password = password;
    host_connection._username = username;
 
    psqlToMongo.start_replication(host_connection);
}

void psql_mongo_replication_cpp_reconnect_mongo_db(int id)
{
    psqlToMongo.reconnect(id);
}

void psql_mongo_replication_cpp_test_linking()
{

}
