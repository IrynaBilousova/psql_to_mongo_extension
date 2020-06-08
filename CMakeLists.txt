cmake_minimum_required (VERSION 3.15.0)

project (psql_mongo_replication LANGUAGES CXX)

if(DEFINED SMAKE_PREFIX_PATH)
    SET(CMAKE_PREFIX_PATH ${SMAKE_PREFIX_PATH};${CMAKE_PREFIX_PATH})
endif()

include(FetchContent)
include(CMakePrintHelpers)
include (externals/cmake_tools.txt)#must be above
include (externals/mongoc-driver.txt)
include (externals/decoder_json.txt)
include (externals/rapidjson.txt)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# psql_mongo_replication
add_subdirectory(pg_recvlogical)
add_subdirectory(psql_mongo_replication)

execute_process(COMMAND export CURRENT_BUILD_DIR=${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(extention_install COMMAND make install
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/psql_to_mongo)