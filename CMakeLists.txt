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

# ---------------------------------------------------------------------------
# Optional nanobind extension module (python/mdfeed_ext.cpp).
#
# Off by default so the C++ side still configures on a machine with no Python
# development headers. Turn it on with -DBUILD_PYTHON_BINDING=ON; nanobind is
# located through the interpreter that pip installed it into, so there is no
# vendored copy and no submodule.
# ---------------------------------------------------------------------------
option(BUILD_PYTHON_BINDING "Build the nanobind zero-copy Python extension" OFF)

if(BUILD_PYTHON_BINDING)
  find_package(Python 3.9 COMPONENTS Interpreter Development.Module REQUIRED)
  execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import nanobind, sys; sys.stdout.write(nanobind.cmake_dir())"
    OUTPUT_VARIABLE NANOBIND_CMAKE_DIR
    RESULT_VARIABLE NANOBIND_FOUND_RC
    ERROR_QUIET)
  if(NOT NANOBIND_FOUND_RC EQUAL 0)
    message(FATAL_ERROR "nanobind not importable by ${Python_EXECUTABLE}. Run: pip install nanobind")
  endif()
  list(APPEND CMAKE_PREFIX_PATH "${NANOBIND_CMAKE_DIR}")
  find_package(nanobind CONFIG REQUIRED)

  nanobind_add_module(mdfeed_ext STABLE_ABI NB_STATIC python/mdfeed_ext.cpp)
  target_include_directories(mdfeed_ext PRIVATE include)
  target_link_libraries(mdfeed_ext PRIVATE Threads::Threads)
  if(WIN32)
    target_link_libraries(mdfeed_ext PRIVATE ws2_32)
  endif()
endif()
