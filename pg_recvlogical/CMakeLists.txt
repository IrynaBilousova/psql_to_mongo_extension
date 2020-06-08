cmake_minimum_required (VERSION 3.15.0)

project (pg_recvlogical LANGUAGES C)

add_library(pg_recvlogical STATIC)

target_include_directories(pg_recvlogical
    INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_sources(pg_recvlogical
    PUBLIC
    include/pg_recvlogical/pg_recvlogical.h
    PRIVATE
    src/pg_recvlogical/pg_recvlogical.c
    src/pg_recvlogical/streamutil.c
    src/pg_recvlogical/streamutil.h
    src/pg_recvlogical/receivelog.h
    src/pg_recvlogical/receivelog.c
    src/pg_recvlogical/walmethods.h
    src/pg_recvlogical/walmethods.c
)

include_directories(
    ${INCLUDEDIR-SERVER}
    ${PKGINCLUDEDIR}
    ${INCLUDEDIR}
    ${INCLUDEDIR}/internal
)

if(NOT DEFINED PKGLIBDIR)
    SET(PKGLIBDIR ${SMAKE_PREFIX_PATH})
endif()

if(NOT DEFINED PSQLLIBS)
    SET(PSQLLIBS ${SMAKE_PREFIX_PATH})
endif()

message(PKGLIBDIR "-${PKGLIBDIR}")
message(PSQLLIBS "${LIBS}")

find_library(pgport_library pgport_shlib HINTS ${PKGLIBDIR})

find_library(pgcommon_library pgcommon_shlib HINTS ${PKGLIBDIR})

find_library(pgfeutils_library pgfeutils HINTS ${PKGLIBDIR})

target_link_libraries(pg_recvlogical PUBLIC ${pgcommon_library} ${pgport_library} pq)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(psql_mongo_replication::pg_recvlogical ALIAS pg_recvlogical)
