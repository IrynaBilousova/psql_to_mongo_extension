#include "psql_mongo_replication/psql_mongo_replication.hpp"
#include "psql_mongo_replication/mongo_replication.hpp"
#include "pg_recvlogical/pg_recvlogical.h" // TODO find way to include path include_directories doesnt work
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "stdafx.hpp"

namespace
{
    enum ACTION_ID
    {
        ACTION_INSERT,
        ACTION_UPDATE,
        ACTION_DELETE,
    };

    void apply_changes(rapidjson::Document& d, psql_mongo_replication::mongo_replication* subsriber)
    {
        rapidjson::Value &r = d["r"];

        std::string str = r.GetString();

        size_t pos = str.find(".");

        if (pos != std::string::npos)
            str.erase(0, pos + 1);

        r.SetString(rapidjson::StringRef(str.c_str()));

        rapidjson::Value &a = d["a"];

        ACTION_ID action = (ACTION_ID)a.GetInt();
        std::string collection = d["r"].GetString();

        d.RemoveMember("a");
        d.RemoveMember("r");

        if (action == ACTION_INSERT && d["d"].IsObject())
        {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d["d"].Accept(writer);

            subsriber->insert(collection, std::string(buffer.GetString()));
        }
        else if (action == ACTION_UPDATE && d["d"].IsObject())
        {
            rapidjson::Value &obj = d["d"];
            rapidjson::Value &key = obj["_id"];
            std::cout << "_id:" << (key.IsNull() ? "" : key.GetString()) << std::endl;

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            d["d"].Accept(writer);
            std::string data = buffer.GetString();

            buffer.Clear();

            d["c"].Accept(writer);
            std::string clause = buffer.GetString();

            subsriber->update(collection, data, clause);
        }
        else if (action == ACTION_DELETE)
        {
            rapidjson::Value &obj = d["d"];
            rapidjson::Value &key = obj["_id"];
            std::cout << "_id:" << (key.IsNull() ? "" : key.GetString()) << std::endl;

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            d["c"].Accept(writer);
            std::string clause = buffer.GetString();

            subsriber->deleteDocs(collection, clause);
        }
    }
}

namespace psql_mongo_replication
{

psql_to_mongo::psql_to_mongo() = default;

psql_to_mongo::~psql_to_mongo()
{
    _replication_thread->join();
};

unsigned char psql_to_mongo::on_changes_static(const void* context, const char* changes, unsigned size)
{
    std::cout << "psql_mongo_replication got changes..." << std::endl;
    psql_to_mongo* _this = (psql_to_mongo*)context;

    _this->on_changes(changes, size);

    return 0;
}

psql_mongo_replication::mongo_replication* psql_to_mongo::get_db_instance(int id)
{
    std::cout << "get_db_instance " << id << std::endl;

    for(auto& subscriber: _mongo_replications_db)
    {
        std::cout << "subscriber ptr: " << subscriber.get() << std::endl;

        if(subscriber && subscriber->get_id() == id)
            return subscriber.get();
    }

    return nullptr;
}

void psql_to_mongo::on_changes(const char* changes, unsigned size)
{
    rapidjson::Document d;

    d.Parse(changes, size);

    std::cout << changes << std::endl;

    rapidjson::Value& subsribers = d["subsribers"];

    for (rapidjson::SizeType i = 0; i < subsribers.Size(); i++) // Uses SizeType instead of size_t
    {
        int id_subsriber = subsribers[i].GetInt();
        std::cout << "subsribers[" << i << "] =" << id_subsriber << std::endl;

        std::lock_guard<std::mutex> lock(_mutex);

        psql_mongo_replication::mongo_replication* subsriber = get_db_instance(id_subsriber);

        if(subsriber == nullptr || !subsriber->connected()) continue;

        apply_changes(d, subsriber);
    }
}

void psql_to_mongo::connect_to_mongo_db(const pg_recvlogical_connection_settings_t& connection)
{
    std::lock_guard<std::mutex> lock(_mutex);

    if(get_db_instance(connection._id))
        return;

    _mongo_replications_db.push_back(std::make_unique<mongo_replication>(connection));

    _mongo_replications_db.front()->test();
}

void psql_to_mongo::reconnect(int id)
{
    _mongo_replications_db.front()->test();
}

void psql_to_mongo::unconnect_from_mongo_db()
{

}

void psql_to_mongo::connect_to_mongo_dbs(const pg_recvlogical_connection_settings_t* connection, unsigned count)
{
    for(size_t i = 0; i < count; ++i)
        connect_to_mongo_db(connection[i]);
}

void psql_to_mongo::start_replication(const pg_recvlogical_connection_settings_t& host_connection)
{
    std::cout << "psql_to_mongo_init..." << std::endl;

    if(_replication_thread) return;

    pg_recvlogical_init_settings_t settings;

    settings._verbose = true;
    settings._repication._plugin = NULL;
    settings._repication._slot = "custom_slot";//"custom_slot";
    settings._connection._dbname = "json_repl";
    settings._connection._host = NULL;
    settings._connection._password = NULL;
    settings._connection._port = NULL;
    settings._connection._username = NULL;

    //settings._connection = host_connection;

    pg_recvlogical_init(&settings, NULL);

    _replication_thread.reset( new std::thread(&pg_recvlogical_stream_logical_start, this, std::ref(on_changes_static)) );
}

}
