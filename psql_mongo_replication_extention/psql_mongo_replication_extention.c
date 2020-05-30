#include "include/psql_mongo_replication_extention/psql_mongo_replication_extention.h"
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "stdint.h"

PG_MODULE_MAGIC;

static void init_extention()
{
    int ret = SPI_exec("CREATE SCHEMA IF NOT EXISTS psql_to_mongo_replication;", 0);

    if(ret < 0)
    {
        elog(ERROR, "_PG_init: SPI_exec returned %s", SPI_result_code_string(ret));
        return;
    }

    ret = SPI_exec("CREATE TABLE IF NOT EXISTS psql_to_mongo_replication.subscribers(id_mongo_db integer, db_name varchar(10), db_hostname varchar(20), pswd varchar(20), username varchar(20));", 0);

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

    int proc = SPI_processed;

    if (SPI_tuptable == NULL)
    {
        elog(INFO, "psql_to_mongo table doesn't exist create it now...");
        ret = SPI_exec("CREATE TABLE psql_to_mongo_replication.subscribers(id_mongo_db integer, db_name varchar(10), db_hostname varchar(20), pswd varchar(20), username varchar(20));", 0);
        if(ret < 0)
        {
            elog(ERROR, "_PG_init: SPI_exec returned %s", SPI_result_code_string(ret));
            return;
        }
    }
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

static int psql_to_mongo_get_mongo_db_count()
{
    int ret = SPI_exec("select * from psql_to_mongo_replication.subscribers;", 0);
    if(ret < 0)
    {
        return ret;
    }

    return SPI_processed;
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

    text *str_db_name = PG_GETARG_TEXT_PP(0);

    char* db_name = VARDATA_ANY(str_db_name);

    text *str_db_hostname = PG_GETARG_TEXT_PP(1);

    char* db_hostname = VARDATA_ANY(str_db_hostname);

    text *str_pswd = PG_GETARG_TEXT_PP(2);

    char* pswd = VARDATA_ANY(str_pswd);

    text *str_username = PG_GETARG_TEXT_PP(3);

    char* username = VARDATA_ANY(str_username);

    char s[200];
    sprintf(s, "INSERT INTO psql_to_mongo_replication.subscribers VALUES (%d, '%s', '%s', '%s', '%s');", mongo_last_db_id, db_name, db_hostname, pswd, username);

    elog(INFO, "psql_to_mongo_add_mongo_db: SPI_exec: %s", s);

    int ret = SPI_exec(s, 0);

    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_add_mongo_db: SPI_exec...error %s", SPI_result_code_string(ret));
    }

    SPI_finish();

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

static void psql_to_mongo_check_if_table_exist_or_create()
{
    int ret = SPI_exec("CREATE TABLE IF NOT EXISTS psql_to_mongo_replication.subscription_info(id_mongo_db integer, pubname varchar(20));", 0);

    if(ret < 0)
    {
        elog(ERROR, "psql_to_mongo_check_if_table_exist_or_create: SPI_exec...error %s", SPI_result_code_string(ret));
    }
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

    psql_to_mongo_check_if_table_exist_or_create();

    int32 mongo_db_id = PG_GETARG_INT32(0);
/*
  pubname  | schemaname | tablename 
-----------+------------+-----------
 mypub     | public     | users
 alltables | public     | books
*/
    text *str = PG_GETARG_TEXT_PP(1);

    char* pubname = VARDATA_ANY(str);

    elog(INFO, "mongo_db_id: %d, pub name: [%s]\n", mongo_db_id, pubname);

    if(psql_to_mongo_is_exist_pg_publication(pubname))
    {
        psql_to_mongo_add_subscription(mongo_db_id, pubname);
    }

    SPI_finish();

    PG_RETURN_VOID();
}

