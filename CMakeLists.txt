# Works with 3.18+
cmake_minimum_required(VERSION 3.18)

# Project name and a few useful settings. Other commands can pick up the results
project(
  DBMS-simulator
  VERSION 0.1
  DESCRIPTION "Database Management System simulator"
  LANGUAGES CXX)

# DEBUG -> all debug symbols
add_compile_options(
  "$<$<CONFIG:DEBUG>:-O0;-g3;-ggdb>"
)

# RELEASE -> optimization enabled
add_compile_options(
  "$<$<CONFIG:RELEASE>:-O3>"
)

# Set a default build type if none was specified
set(default_build_type "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


# add compiler option for specific compiler
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  add_compile_options("-Weverything")
else() # gcc or icc
  add_compile_options("-Wall" "-Wextra")
endif()

# Uncomment this line to set verbose mode in makefile
# set(CMAKE_VERBOSE_MAKEFILE ON)

# C++ options for all compilers
set(CMAKE_CXX_STANDARD 17) # -std=c++17
set(CMAKE_CXX_EXTENSIONS OFF) # turn off gnu

find_package(spdlog REQUIRED)

# Workaround for missing cmake script in gmock / gtest repo
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# make test working on
enable_testing()

find_package(GTest REQUIRED)
find_package(GMock REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${GMOCK_INCLUDE_DIRS})

# The compiled library code is under ./src
add_subdirectory(src)

# The executable code is unser ./app
add_subdirectory(app)

# The tests code is under ./tests
add_subdirectory(tests)

