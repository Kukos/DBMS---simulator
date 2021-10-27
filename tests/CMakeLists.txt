file(GLOB TEST_SRC_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/tests/*.cpp")

add_executable(testcode ${TEST_SRC_LIST})
# LEVEL=0 means TRACE
# LEVEL=1 means DEBUG
# LEVEL=10 means for sure turn it off
target_compile_definitions(testcode PRIVATE LOGGER_ACTIVE_LEVEL=10)

# gtest needs pthread
target_link_libraries(testcode PRIVATE srccode gtest_main gtest ${GMOCK_BOTH_LIBRARIES} pthread spdlog::spdlog)
target_include_directories(testcode PUBLIC ../include)

# This is tricky. We dont need main with Gtest init. This will create binary file with generated main
include(GoogleTest)
gtest_discover_tests(testcode)