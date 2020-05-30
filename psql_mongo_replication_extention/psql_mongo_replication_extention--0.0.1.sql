\echo Use "CREATE EXTENSION psql_to_mongo" to load this file. \quit
SELECT NULL AS "Starting to insert into table X";

CREATE FUNCTION psql_to_mongo_subscribe(integer, text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;


CREATE FUNCTION psql_to_mongo_add_mongo_db(text, text, text, text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;
