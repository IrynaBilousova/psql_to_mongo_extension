\echo Use "CREATE EXTENSION psql_to_mongo" to load this file. \quit
\echo DROP EXTENSION psql_to_mongo; CREATE EXTENSION psql_to_mongo cascade; \quit

SELECT NULL AS "Starting to insert into table X";

CREATE FUNCTION psql_to_mongo_subscribe(integer, text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_unsubscribe(integer) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_add_mongo_db(text, text, text, text, text) RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_connect_to_mongo_db(integer) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_reconnect_to_mongo_db(integer) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_remove_from_mongo_db(integer) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION psql_to_mongo_replication_worker_start(text, text, text, text, text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;