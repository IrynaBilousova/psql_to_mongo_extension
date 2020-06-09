//#include "psql_to_mongo/psql_to_mongo.h"
#include "psql_mongo_replication/psql_to_mongo_c_to_cpp_call_api.h"
#include "postgres.h"
#include "pg_recvlogical/pg_recvlogical.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "stdint.h"

PG_MODULE_MAGIC;

enum { offset_for_function_args = 2};

typedef enum
{
    subscriber_id_column = 1,
    db_name_column,
    host_column,
    port_column,
    username_column,
    pswd_column,
} subscribers_column_id;

typedef enum
{
    subscribers_info_id_column = 2,
    publication_column
} subscribers_info_column_id;

static int psql_to_mongo_get_mongo_db_count()
{
    int ret = SPI_exec("select * from psql_to_mongo_replication.subscribers;", 0);
    if(ret < 0)
    {
        return ret;
    }

    return SPI_processed;
}

#define copyDataFromArg(data, id)\
{\
    VarChar *source = PG_GETARG_VARCHAR_PP(id - offset_for_function_args);\
\
    size_t len = VARSIZE_ANY_EXHDR(source);\
	char* s_data = VARDATA_ANY(source);\
\
    memcpy(data, s_data, len);\
    data[len] = '\0';\
}

static int psql_to_mongo_fill_subscribe_info_from_spi(
      HeapTuple tuple
    , TupleDesc tupdesc
    , char **dbname
    , char **port
    , char **host
    , char **password
    , char **username
    , int* id
    )
{
    _Bool is_null = 0;

    Datum val = SPI_getbinval(tuple, tupdesc, subscriber_id_column, &is_null);

    if (!is_null)
        *id = DatumGetInt64(val);

    *dbname = SPI_getvalue(tuple, tupdesc, db_name_column);
    *port = SPI_getvalue(tuple, tupdesc, port_column);
    *host = SPI_getvalue(tuple, tupdesc, host_column);
    *password = SPI_getvalue(tuple, tupdesc, pswd_column);
    *username = SPI_getvalue(tuple, tupdesc, username_column);
}

static int psql_to_mongo_connect_subscribers()
{
    int ret = SPI_exec("select * from psql_to_mongo_replication.subscribers;", 0);

    elog(INFO, "psql_to_mongo_fill_subscribers_info %d\n", SPI_processed);

    if(ret < 0)
    {
        return ret;
    }

    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;

    int proc = SPI_processed;

    for (int i = 0; i < proc; i++)
    {
        HeapTuple tuple = tuptable->vals[i];

        const char* dbname = NULL; 
        const char* port = NULL;
        const char* host = NULL;
        const char* password = NULL;
        const char* username = NULL;

        int id = 0;

        psql_to_mongo_fill_subscribe_info_from_spi(
            tuple, tupdesc, &dbname, &port, &host, &password, &username, &id);

        elog(INFO, "subscribers[%d] %s->%s:%s {%s %s}", i, dbname, host, port, password, username);

        psql_mongo_replication_cpp_connect_mongo_db(dbname, host, port, username, password, id);
    }

    return SPI_processed;
}

static void psql_to_mongo_check_if_subscription_exist_or_create()
{
    int ret = SPI_exec("CREATE TABLE IF NOT EXISTS psql_to_mongo_replication.subscription_info(subscriber_id integer, pubname varchar(20));", 0);

    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_check_if_table_exist_or_create: SPI_exec...error %s", SPI_result_code_string(ret));
    }
}

static void init_extention()
{
    int ret = SPI_exec("CREATE SCHEMA IF NOT EXISTS psql_to_mongo_replication;", 0);

    if(ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_exec returned %s", SPI_result_code_string(ret));
        return;
    }

    ret = SPI_exec("CREATE TABLE IF NOT EXISTS psql_to_mongo_replication.subscribers(\
          subscriber_id integer PRIMARY KEY\
        , db_name varchar(10)\
        , host varchar(20)\
        , port varchar(10)\
        , username varchar(20)\
        , pswd varchar(20));", 0);

    if (ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
	    return;
    }

    ret = SPI_execute("SELECT to_regclass('psql_to_mongo_replication.subscribers');", true, 0);
    if (ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_execute returned %s", SPI_result_code_string(ret));
	    return;
    }
  
    psql_to_mongo_check_if_subscription_exist_or_create();

    psql_to_mongo_connect_subscribers(); 
} 

void _PG_init()
{
    elog(INFO, "psql_to_mongo extention init...OK");
    SPI_connect();

    init_extention();

    SPI_finish();
}

void _PG_fini(void)
{
    elog(INFO, "psql_to_mongo extention deinit...OK");
}

PG_FUNCTION_INFO_V1(psql_to_mongo_add_mongo_db);

Datum
psql_to_mongo_add_mongo_db(PG_FUNCTION_ARGS)
{
    SPI_connect();

    int mongo_last_db_id = psql_to_mongo_get_mongo_db_count();

    if(mongo_last_db_id < 0)
    {
        elog(ERROR, "psql_to_mongo_add_mongo_db: SPI_exec...error %s", SPI_result_code_string(mongo_last_db_id));
        PG_RETURN_VOID();
    }

    char dbname[NAMEDATALEN];
    copyDataFromArg(dbname, db_name_column);

    char port[NAMEDATALEN];
    copyDataFromArg(port, port_column);

    char host[NAMEDATALEN];
    copyDataFromArg(host, host_column);

    char password[NAMEDATALEN];
    copyDataFromArg(password, pswd_column);

    char username[NAMEDATALEN];
    copyDataFromArg(username, username_column);

    char s[200];
    sprintf(s, "INSERT INTO psql_to_mongo_replication.subscribers VALUES (%d, '%s', '%s', '%s', '%s', '%s');", 
                mongo_last_db_id, dbname, host, port, username, password);

    elog(INFO, "psql_to_mongo_add_mongo_db: SPI_exec: %s", s);

    int ret = SPI_exec(s, 0);

    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_add_mongo_db: SPI_exec...error %s", SPI_result_code_string(ret));
    }

    SPI_finish();

    psql_mongo_replication_cpp_connect_mongo_db(dbname, host, port, username, password, mongo_last_db_id);

    //psql_mongo_replication_cpp_connect_mongo_db("db_name", "127.0.0.1", "27017", "a", "123", 0);

    PG_RETURN_INT32(mongo_last_db_id);
}

PG_FUNCTION_INFO_V1(psql_to_mongo_connect_to_mongo_db);

Datum
psql_to_mongo_connect_to_mongo_db(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_connect_to_mongo_db: %d\n", PG_GETARG_INT32(0));

    int id = PG_GETARG_INT32(0);
    char query[200]; 

    sprintf(query, "select * from psql_to_mongo_replication.subscribers where \
        psql_to_mongo_replication.subscribers.subscriber_id = %d;", id);

    SPI_connect();

    int ret = SPI_exec(query, 0);

    const char *dbname = NULL;
    const char *port = NULL;
    const char *host = NULL;
    const char *password = NULL;
    const char *username = NULL;

    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    HeapTuple tuple = tuptable->vals[0];

    psql_to_mongo_fill_subscribe_info_from_spi(
        tuple, tupdesc, &dbname, &port, &host, &password, &username, &id);

    elog(INFO, "subscribers[%d] %s->%s:%s {%s:%s}", id, dbname, host, port, username, password);

    psql_mongo_replication_cpp_connect_mongo_db(dbname, host, port, username, password, id);
    
    SPI_finish();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(psql_to_mongo_reconnect_to_mongo_db);

Datum 
psql_to_mongo_reconnect_to_mongo_db(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_reconnect_to_mongo_db: %d\n", PG_GETARG_INT32(0));

    int id = PG_GETARG_INT32(0);

    //psql_mongo_replication_cpp_reconnect_mongo_db(id);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(psql_to_mongo_remove_from_mongo_db);

Datum
psql_to_mongo_remove_from_mongo_db(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_remove_from_mongo_db: %d\n", PG_GETARG_INT32(0));

    PG_RETURN_VOID();
}

static int psql_to_mongo_is_exist_pg_publication(const char* pubname)
{
    char query[100]; 
    sprintf(query, "select * from pg_publication_tables where pubname = '%s';", pubname);
    elog(INFO, "psql_to_mongo_is_exist_pg_publication: query [%s]", query);

    int ret = SPI_exec(query, 0);

    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_is_exist_pg_publication: SPI_exec...error %s", SPI_result_code_string(ret));
        return ret;
    }

    return SPI_processed;
}

static void psql_to_mongo_add_subscription(int mongo_last_db_id, const char* pubname)
{
    char query[100]; 
    sprintf(query, "INSERT into psql_to_mongo_replication.subscription_info VALUES (%d,'%s');", mongo_last_db_id, pubname);

    elog(INFO, "psql_to_mongo_add_subscription: query [%s]", query);

    int ret = SPI_exec(query, 0);
    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_add_subscription: SPI_exec...error %s", SPI_result_code_string(ret));
    }
}

PG_FUNCTION_INFO_V1(psql_to_mongo_subscribe);

Datum
psql_to_mongo_subscribe(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_subscribe: ");
 
    if(PG_NARGS() != 2)
    {   
        elog(ERROR, "psql_to_mongo_subscribe: PG_NARGS to few %d", PG_NARGS());
        PG_RETURN_VOID();
    }

    SPI_connect();

    int32 mongo_db_id = PG_GETARG_INT32(0);
/*
  pubname  | schemaname | tablename 
-----------+------------+-----------
 mypub     | public     | users
 alltables | public     | books
*/
    text *str = PG_GETARG_TEXT_PP(1);

    char pubname[NAMEDATALEN];
    copyDataFromArg(pubname, publication_column);

    elog(INFO, "mongo_db_id: %d, pub name: [%s]\n", mongo_db_id, pubname);

    if(psql_to_mongo_is_exist_pg_publication(pubname))
    {
        psql_to_mongo_add_subscription(mongo_db_id, pubname);
    }

    SPI_finish();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(psql_to_mongo_unsubscribe);

Datum
psql_to_mongo_unsubscribe(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_unsubscribe: %d\n", PG_GETARG_INT32(0));

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(psql_to_mongo_replication_worker_start);

Datum
psql_to_mongo_replication_worker_start(PG_FUNCTION_ARGS)
{
    elog(INFO, "psql_to_mongo_replication_worker_start: \n");

    if(PG_NARGS() != 5)
    {
        elog(ERROR, "psql_to_mongo_replication_worker_start: PG_NARGS to few %d", PG_NARGS());
        PG_RETURN_VOID();
    }

    char dbname[NAMEDATALEN];
    copyDataFromArg(dbname, db_name_column);

    char port[NAMEDATALEN];
    copyDataFromArg(port, port_column);

    char host[NAMEDATALEN];
    copyDataFromArg(host, host_column);

    char username[NAMEDATALEN];
    copyDataFromArg(username, username_column);

    char password[NAMEDATALEN];
    copyDataFromArg(password, pswd_column);

    elog(INFO, "psql_mongo_replication_cpp_start_replication: '%s', '%s', '%s', '%s', '%s'\n", dbname, host, port, username, password);

    psql_mongo_replication_cpp_start_replication(
          dbname
        , host
        , port
        , username
        , password);

    elog(INFO, "psql_mongo_replication_cpp_started_replication: SUCCESS\n");

    PG_RETURN_VOID();
}