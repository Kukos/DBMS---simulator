#include <bar.hpp>
#include <iostream>

#include <logger/logger.hpp>

int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));

    std::cout << "Example" << std::endl;
    std::cout << bar(10) << std::endl;

    return 0;
}
