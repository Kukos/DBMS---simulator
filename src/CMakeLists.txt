file(GLOB HEADER_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/include/*/*.hpp")
file(GLOB HEADER_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/include/*.hpp")

file(GLOB SRC_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/src/*.cpp")

add_library(srccode ${SRC_LIST} ${HEADER_LIST})

target_include_directories(srccode PUBLIC ../include)