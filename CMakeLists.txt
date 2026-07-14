cmake_minimum_required(VERSION 3.15)
project(exchange_market_data_decoder CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(MSVC)
  add_compile_options(/O2 /W4 /EHsc)
else()
  add_compile_options(-O3 -Wall)
endif()

include_directories(include)

add_executable(decoder_bench src/decoder_bench.cpp)
add_executable(correctness_test tests/correctness_test.cpp)

add_executable(live_multicast_demo src/live_multicast_demo.cpp)
if(WIN32)
  target_link_libraries(live_multicast_demo ws2_32)
endif()

find_package(Threads REQUIRED)
target_link_libraries(decoder_bench Threads::Threads)
target_link_libraries(correctness_test Threads::Threads)
target_link_libraries(live_multicast_demo Threads::Threads)
