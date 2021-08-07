#include <bar.hpp>
#include <gtest/gtest.h>

GTEST_TEST(barTestSuite1, barTest1)
{
    EXPECT_EQ(bar(1), 2);
    EXPECT_EQ(bar(2), 4);
    EXPECT_EQ(bar(5), 10);
}