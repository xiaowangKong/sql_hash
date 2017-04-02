cmake_minimum_required(VERSION 3.6)
project(sql_hash)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp GroupingBuffer.h GroupingBuffer.cpp murmurhash.h MyHash.cpp MyHash.h timer.h)
add_executable(sql_hash ${SOURCE_FILES})