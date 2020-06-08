#include "psql_mongo_replication/psql_mongo_replication.hpp"
#include "psql_mongo_replication/psql_to_mongo_c_to_cpp_call_api.h"
#include "pg_recvlogical/pg_recvlogical.h"
#include "stdafx.hpp"

int main(int argc, char **argv)
{
    psql_mongo_replication_cpp_connect_mongo_db("db_name", "127.0.0.1", "27017", "a", "123", 0);

    psql_mongo_replication_cpp_start_replication("db_name", "127.0.0.1", "27017", "a", "123");

    char a = 0;

    while(a != 'q') { std::cin >> a; };

    return 0;
}