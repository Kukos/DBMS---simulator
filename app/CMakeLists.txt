file(GLOB APP_LIST CONFIGURE_DEPENDS "${DBMS-simulator_SOURCE_DIR}/app/*.cpp")

add_executable(main.out ${APP_LIST})

# LEVEL=2 means INFO
target_compile_definitions(main.out PRIVATE LOGGER_ACTIVE_LEVEL=2)

target_link_libraries(main.out PRIVATE srccode spdlog::spdlog)
target_include_directories(main.out PUBLIC ../include)