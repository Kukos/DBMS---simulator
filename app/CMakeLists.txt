file(GLOB APP_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/app/*.cpp")

add_executable(main.out ${APP_LIST})

target_link_libraries(main.out PRIVATE srccode)
