#include "psql_mongo_replication/mongo_replication.hpp"
#include "pg_recvlogical/pg_recvlogical.h"
#include <mongoc.h>
#include "stdafx.hpp"
namespace
{
static void print_bson (const bson_t *b)
{
   char *str;

   str = bson_as_canonical_extended_json (b, NULL);
   fprintf (stdout, "%s\n", str);
   bson_free (str);
}

// find()
// {
//     mongoc_cursor_t *cursor = mongoc_collection_find_with_opts (collection, filter, NULL, NULL);

//     if(mongoc_cursor_error (cursor, &error))
//     {
//         std::cout << error.message << std::endl;
//         return;
//     }

//     const bson_t *doc;

//     mongoc_cursor_next (cursor, &doc);

//     print_bson (doc);

//     bson_oid_t oid;

//    bson_iter_t iter;

//     if (bson_iter_init (&iter, doc)) 
//     {
//         const bson_value_t *value;

//         value = bson_iter_value (&iter);

//         if(value->value_type == BSON_TYPE_OID);
//             oid = value->value.v_oid;
//     }
// }

int mongo_test (mongoc_client_t* client, const std::string& db_name)
{
    mongoc_database_t *database;
    mongoc_collection_t *collection;
    bson_t *command, reply, *insert;
    char *str;
    bool retval;

    /*
        * Get a handle on the database "db_name" and collection "coll_name"
        */
    database = mongoc_client_get_database (client, db_name.c_str());
    collection = mongoc_client_get_collection (client,  db_name.c_str(), "coll_name");

    /*
        * Do work. This example pings the database, prints the result as JSON and
        * performs an insert
        */
    command = BCON_NEW ("ping", BCON_INT32 (1));

    bson_error_t error;

    printf ("mongoc_client_command_simple\n");

    retval = mongoc_client_command_simple (
        client, "admin", command, NULL, &reply, &error);

    if (!retval) {
        printf ("%s\n", error.message);
        return EXIT_FAILURE;
    }

    str = bson_as_json (&reply, NULL);
    printf ("bson_as_json: %s\n", str);

    insert = BCON_NEW ("hello", BCON_UTF8 ("world"));

    if (!mongoc_collection_insert_one (collection, insert, NULL, NULL, &error)) {
        fprintf (stderr, "%s\n", error.message);
    }

    bson_destroy (insert);
    bson_destroy (&reply);
    bson_destroy (command);
    bson_free (str);

    /*
        * Release our handles and clean up libmongoc
        */
    mongoc_collection_destroy (collection);
    mongoc_database_destroy (database);

    return EXIT_SUCCESS;
}

mongoc_client_t* init(const std::string& uri_string, mongoc_uri_t *uri)
{
     std::cout << "mongoc_client_t init" << std::endl;
    /*
    * Required to initialize libmongoc's internals
    */
    mongoc_init ();

    /*
    * Safely create a MongoDB URI object from the given string
    */
    bson_error_t error;

    uri = mongoc_uri_new_with_error (uri_string.c_str(), &error);

    if (!uri) 
    {
        std::cout << "failed to parse URI:"<< uri_string << "\n"
                << "error message:" << error.message << "\n";
        return NULL;
    }

    /*
    * Create a new client instance
    */
    return mongoc_client_new_from_uri (uri);
}

//mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
std::string make_uri(      
      const char* dbname
    , const char* port
    , const char* host
    , const char* username
    , const char* password)
{
    std::stringstream ss;

    ss << "mongodb://";

    if(username && password)
       ss << username << ":" << password << "@";
 
    ss << host <<":"<< port << "/" << dbname;

    return ss.str();
}

}
namespace psql_mongo_replication
{

mongo_replication::mongo_replication(const pg_recvlogical_connection_settings_t& connection): 
      _db_name(connection._dbname)
    , _id(connection._id)
{
    const std::string uri_string = make_uri(
          connection._dbname
        , connection._port
        , connection._host
        , connection._username
        , connection._password);

    std::cout << "uri_string: " << uri_string << std::endl;

    _client = init(uri_string, _uri);
    
    /*
    * Register the application name so we can track it in the profile logs
    * on the server. This can also be done from the URI (see other examples).
    */
    mongoc_client_set_appname (_client, "connect-example");
}

mongo_replication::~mongo_replication()
{
    mongoc_uri_destroy (_uri);
    mongoc_client_destroy (_client);
    mongoc_cleanup ();

    std::cout << "~mongo_replication" << std::endl;
}

void mongo_replication::insert(const std::string& collectionName, const std::string& changes)
{
    bson_error_t error;
    mongoc_database_t *database = mongoc_client_get_database (_client, _db_name.c_str());
    mongoc_collection_t *collection = mongoc_client_get_collection (_client, _db_name.c_str(), collectionName.c_str());

    std::cout << "mongo_replication insert: "<< collectionName << ":" << changes << std::endl;

    bson_t *insert = bson_new_from_json ((const uint8_t *)changes.c_str(), changes.size(), &error);

    if (!mongoc_collection_insert_one (collection, insert, NULL, NULL, &error)) 
    {
        std::cout << error.message << std::endl;
        return;
    }
}

void mongo_replication::update(const std::string& collectionName, const std::string& changes, const std::string& clause)
{
    bson_error_t error;
    mongoc_database_t *database = mongoc_client_get_database (_client, _db_name.c_str());
    mongoc_collection_t *collection = mongoc_client_get_collection (_client, _db_name.c_str(), collectionName.c_str());

    std::cout << "update << "<< collectionName << ":" << changes << "->" << clause << std::endl;

    bson_t * query = bson_new_from_json ((const uint8_t *)clause.c_str(), clause.size(), &error);

    bson_t *update = bson_new_from_json ((const uint8_t *)changes.c_str(), changes.size(), &error);

    if (!mongoc_collection_update(collection, MONGOC_UPDATE_NONE, query, update, NULL, &error)) 
    {
        std::cout << error.message << std::endl;
        return;
    }
}

void mongo_replication::deleteDocs(const std::string& collectionName, const std::string& clause)
{
    bson_error_t error;
    mongoc_database_t *database = mongoc_client_get_database (_client, _db_name.c_str());
    mongoc_collection_t *collection = mongoc_client_get_collection (_client, _db_name.c_str(), collectionName.c_str());

    std::cout << "delete << "<< collectionName << "->" << clause << std::endl;

    bson_t * query = bson_new_from_json ((const uint8_t *)clause.c_str(), clause.size(), &error);

    if (!mongoc_collection_remove (collection, MONGOC_REMOVE_NONE, query, NULL, &error))
    {
        std::cout << error.message << std::endl;
        return;
    }
}

unsigned int mongo_replication::get_id()
{
    return _id;
}

bool mongo_replication::connected()
{
    return mongoc_client_get_database (_client, _db_name.c_str()) != nullptr;
}

void mongo_replication::test()
{
    std::cout << __FUNCTION__ << std::endl;

    mongo_test(_client, _db_name);
}


}