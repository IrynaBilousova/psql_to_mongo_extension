#pragma once

#include <string>

struct _mongoc_uri_t;
struct _mongoc_client_t;

namespace psql_mongo_replication
{
    class mongo_replication
    {
        private:
        _mongoc_uri_t *_uri;
        _mongoc_client_t *_client;
        std::string _db_name;

        public:
        mongo_replication();
        ~mongo_replication();

        void insert(const std::string& collectionName, const std::string& changes);
        void update(const std::string& collectionName, const std::string& changes, const std::string& clause);
        void deleteDocs(const std::string& collectionName, const std::string& clause);
        void test();
    };
}


