#include <iostream>

#include <logger/logger.hpp>

int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));

    std::cout << "Database Management System - simulator" << std::endl;

    return 0;
}
