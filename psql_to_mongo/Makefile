EXTENSION = psql_to_mongo        # the extensions name
DATA = psql_to_mongo--0.0.1.sql  # script files to install
MODULE_big = psql_to_mongo

path_to_pg_recvlogical=../pg_recvlogical/src/pg_recvlogical

OBJS = src/psql_to_mongo.o 
OBJS += $(path_to_pg_recvlogical)/pg_recvlogical.o $(path_to_pg_recvlogical)/receivelog.o $(path_to_pg_recvlogical)/streamutil.o $(path_to_pg_recvlogical)/walmethods.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PKGLIBDIR = /usr/lib/postgresql/12/lib

SHLIB_LINK +=  ../build/psql_mongo_replication/libpsql_mongo_replication_lib.a
SHLIB_LINK +=  /usr/lib/x86_64-linux-gnu/libbson-1.0.so.0   /usr/lib/x86_64-linux-gnu/libmongoc-1.0.so.0 
SHLIB_LINK += -lpq -lpthread -L$(PKGLIBDIR)/ -lpgport_shlib -lpgcommon_shlib

PG_CPPFLAGS += -I../include/ -I../psql_mongo_replication/include -I../pg_recvlogical/include
PG_CPPFLAGS += -I/usr/include/postgresql/ -DEXTENTION_BUILD

include $(PGXS)

#psql_mongo_replication_extention.bc : src/psql_mongo_replication_extention.c
#	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ $< -lstdc++ 