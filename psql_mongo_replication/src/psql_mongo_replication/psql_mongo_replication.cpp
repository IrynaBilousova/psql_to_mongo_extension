#include "psql_mongo_replication/psql_mongo_replication.hpp"
#include "psql_mongo_replication/mongo_replication.hpp"
#include "/home/a/Desktop/shared_data/psql_mongo_replication/pg_recvlogical/include/pg_recvlogical/pg_recvlogical.h" // TODO find way to include path include_directories doesnt work
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "stdafx.hpp"

namespace psql_mongo_replication
{

enum ACTION_ID
{
    ACTION_INSERT,
    ACTION_UPDATE,
    ACTION_DELETE,
};

psql_to_mongo::psql_to_mongo() = default;

psql_to_mongo::~psql_to_mongo() = default;

unsigned char psql_to_mongo::on_changes_static(const void* context, const char* changes, unsigned size)
{
    std::cout << "psql_mongo_replication got changes..." << std::endl;
    psql_to_mongo* _this = (psql_to_mongo*)context;

    _this->on_changes(changes, size);

    return 0;
}

void psql_to_mongo::on_changes(const char* changes, unsigned size)
{
    rapidjson::Document d;

    d.Parse(changes, size);

    std::cout << changes << std::endl;

    rapidjson::Value& r = d["r"];

    std::string str = r.GetString();

    size_t pos = str.find(".");

    if(pos != std::string::npos)
        str.erase(0, pos + 1);

    r.SetString(rapidjson::StringRef(str.c_str()));

    rapidjson::Value& a = d["a"];

    ACTION_ID action = (ACTION_ID)a.GetInt();
    std::string collection = d["r"].GetString();

    d.RemoveMember("a");
    d.RemoveMember("r");

    if(action == ACTION_INSERT && d["d"].IsObject())
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d["d"].Accept(writer);

        _mongo_replication->insert(collection, std::string(buffer.GetString()));
    }
    else if(action == ACTION_UPDATE && d["d"].IsObject())
    {
        rapidjson::Value& obj = d["d"];
        rapidjson::Value& key = obj["_id"];
        std::cout << "_id:" << (key.IsNull()? "": key.GetString()) << std::endl;

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        d["d"].Accept(writer);
        std::string data = buffer.GetString();

        buffer.Clear();

        d["c"].Accept(writer);
        std::string clause = buffer.GetString();

        _mongo_replication->update(collection, data, clause);
    }
    else if(action == ACTION_DELETE)
    {
        rapidjson::Value& obj = d["d"];
        rapidjson::Value& key = obj["_id"];
        std::cout << "_id:" << (key.IsNull()? "": key.GetString()) << std::endl;

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        d["c"].Accept(writer);
        std::string clause = buffer.GetString();

        _mongo_replication->deleteDocs(collection, clause);
    }
}

void psql_to_mongo::init(int argc, char **argv)
{
    std::cout << "psql_to_mongo_init..." << std::endl;

    _mongo_replication = std::make_unique<mongo_replication>();

    _mongo_replication->test();

    pg_recvlogical_init(argc, argv);

    pg_recvlogical_stream_logical_start(this, &on_changes_static);
}

}
