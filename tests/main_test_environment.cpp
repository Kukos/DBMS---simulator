#include <logger/logger.hpp>

#include <gtest/gtest.h>

class TestEnvironment : public ::testing::Environment
{
public:
    // call once per test binary
    void SetUp() override
    {
        loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));
    }
};

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new TestEnvironment);

    return RUN_ALL_TESTS();
}
