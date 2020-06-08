#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <vector>

struct pg_recvlogical_connection_settings_t;

namespace psql_mongo_replication
{
    class mongo_replication;

    class psql_to_mongo
    {
        private:
        std::vector<std::unique_ptr<mongo_replication>> _mongo_replications_db;
        static unsigned char on_changes_static(const void* context, const char* changes, unsigned size);
        psql_mongo_replication::mongo_replication* get_db_instance(int id);
        std::unique_ptr<std::thread> _replication_thread;
        std::mutex _mutex;

        public:
        psql_to_mongo();
        ~psql_to_mongo();
        void on_changes(const char* changes, unsigned size);
        void connect_to_mongo_db(const pg_recvlogical_connection_settings_t& connection);
        void reconnect(int id);
        void unconnect_from_mongo_db();
        void connect_to_mongo_dbs(const pg_recvlogical_connection_settings_t* connection, unsigned count);
        void start_replication(const pg_recvlogical_connection_settings_t& host_connection);
    };
}
