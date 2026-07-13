cmake_minimum_required(VERSION 3.15)
project(questdb_ha_sender_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Path to a c-questdb-client checkout (v7.0.0+ / main, with QWP + egress).
# Override with -DQUESTDB_CLIENT_DIR=/path/to/c-questdb-client.
set(QUESTDB_CLIENT_DIR "/Users/j/prj/questdb/c-questdb-client"
    CACHE PATH "c-questdb-client checkout")

# The egress reader (query client / probe) needs the reader surface, and the probe
# uses tls_verify=unsafe_off; enable both cargo features via the client's CMake options.
set(QUESTDB_ENABLE_READER ON CACHE BOOL "" FORCE)
set(QUESTDB_ENABLE_INSECURE_SKIP_VERIFY ON CACHE BOOL "" FORCE)

add_subdirectory(${QUESTDB_CLIENT_DIR} ${CMAKE_BINARY_DIR}/questdb_client_build)

find_package(ZLIB REQUIRED)

add_executable(csv_parallel_sender csv_parallel_sender.cpp)
target_include_directories(csv_parallel_sender PRIVATE ${QUESTDB_CLIENT_DIR}/include)
target_link_libraries(csv_parallel_sender PRIVATE questdb_client ZLIB::ZLIB)

find_package(Threads REQUIRED)
target_link_libraries(csv_parallel_sender PRIVATE Threads::Threads)
