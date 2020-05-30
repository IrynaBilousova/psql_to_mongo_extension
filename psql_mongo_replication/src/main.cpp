#include "psql_mongo_replication/psql_mongo_replication.hpp"
#include "stdafx.hpp"

int main(int argc, char **argv)
{
    psql_mongo_replication::psql_to_mongo psqlToMongo;
    psqlToMongo.init(argc, argv);

    return 0;
}