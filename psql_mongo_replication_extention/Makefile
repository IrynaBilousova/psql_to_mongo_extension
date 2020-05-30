EXTENSION = psql_mongo_replication_extention        # the extensions name
DATA = psql_mongo_replication_extention--0.0.1.sql  # script files to install
#REGRESS = psql_mongo_replication_extention_test     # our test script file (without extension)
SHLIB_LINK = -lstdc++
#OBJS = psql_mongo_replication_extention.so
MODULES = psql_mongo_replication_extention
# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#psql_mongo_replication_extention.bc : src/psql_mongo_replication_extention.c
#	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ $<