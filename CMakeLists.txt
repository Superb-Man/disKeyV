cmake_minimum_required(VERSION 3.16)
project(disKeyV LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(diskeyv_options INTERFACE)
target_include_directories(diskeyv_options INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(diskeyv_options INTERFACE
    -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
target_link_libraries(diskeyv_options INTERFACE Threads::Threads)

add_executable(diskeyv-server main.cpp)
target_link_libraries(diskeyv-server PRIVATE diskeyv_options)

add_executable(diskeyv-client cl.cpp)
target_link_libraries(diskeyv-client PRIVATE diskeyv_options)

include(CTest)
if(BUILD_TESTING)
    add_executable(diskeyv-tests tests/test_correctness.cpp)
    target_link_libraries(diskeyv-tests PRIVATE diskeyv_options)
    add_test(NAME diskeyv-correctness COMMAND diskeyv-tests)
    set_tests_properties(diskeyv-correctness PROPERTIES TIMEOUT 20)
endif()

option(DISKEYV_ENABLE_SANITIZERS "Enable address and undefined behavior sanitizers" OFF)
if(DISKEYV_ENABLE_SANITIZERS)
    target_compile_options(diskeyv_options INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(diskeyv_options INTERFACE -fsanitize=address,undefined)
endif()
