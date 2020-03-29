cmake_minimum_required(VERSION 3.12)
project(sort)

set(CMAKE_CXX_STANDARD 20)

add_executable(sort main.cpp)

target_link_libraries(sort PRIVATE pthread stdc++fs)

target_include_directories(sort PRIVATE .)
