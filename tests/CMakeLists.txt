file(GLOB TEST_SRC_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/tests/*.cpp")

add_executable(testcode ${TEST_SRC_LIST})

# gtest needs pthread
target_link_libraries(testcode PRIVATE srccode gtest_main gtest ${GMOCK_BOTH_LIBRARIES} pthread)

# This is tricky. We dont need main with Gtest init. This will create binary file with generated main
include(GoogleTest)
gtest_discover_tests(testcode)