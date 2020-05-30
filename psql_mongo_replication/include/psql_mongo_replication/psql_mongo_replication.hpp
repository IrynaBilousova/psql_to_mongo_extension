#pragma once

#include <memory>

namespace psql_mongo_replication
{
    class mongo_replication;

    class psql_to_mongo
    {
        private:
        std::unique_ptr<mongo_replication> _mongo_replication;
        static unsigned char on_changes_static(const void* context, const char* changes, unsigned size);

        public:
        psql_to_mongo();
        ~psql_to_mongo();
        void on_changes(const char* changes, unsigned size);
        void init(int argc, char **argv);
    };
}
