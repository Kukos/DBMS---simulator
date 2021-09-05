#include <observability/counterManager.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(counterManagerBasicTest, interface)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};


    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);
    }

    counterManager_int.resetCounter(counterId[2]);
    counterManager_int.resetAllCounters();

    counterManager_double.resetCounter(counterId[1]);
    counterManager_double.resetAllCounters();
}

GTEST_TEST(counterManagerBasicTest, defaultPeg)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i]);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 1);

        counterManager_int.pegCounter(counterId[i]);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 2);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i]);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 1.0);

        counterManager_double.pegCounter(counterId[i]);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 2.0);
    }
}

GTEST_TEST(counterManagerBasicTest, valuePeg)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i], 5);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 5);

        counterManager_int.pegCounter(counterId[i], 111);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 116);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i], 5.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 5.0);

        counterManager_double.pegCounter(counterId[i], 111.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 116.0);
    }
}

GTEST_TEST(counterManagerBasicTest, copy)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i], 5);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 5);

        counterManager_int.pegCounter(counterId[i], 111);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 116);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i], 5.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 5.0);

        counterManager_double.pegCounter(counterId[i], 111.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 116.0);
    }

    CounterManager<int> intCopy(counterManager_int);
    for (size_t i = 0; i < arrSize; ++i)
    {
        EXPECT_EQ(intCopy.getCounter(counterId[i]), std::make_pair(counterName[i], 116));
        EXPECT_EQ(intCopy.getCounterValue(counterId[i]), 116);
        EXPECT_EQ(intCopy.getCounterName(counterId[i]), counterName[i]);
    }

    CounterManager<int> intCopy2;
    intCopy2 = intCopy;

    for (size_t i = 0; i < arrSize; ++i)
    {
        EXPECT_EQ(intCopy2.getCounter(counterId[i]), std::make_pair(counterName[i], 116));
        EXPECT_EQ(intCopy2.getCounterValue(counterId[i]), 116);
        EXPECT_EQ(intCopy2.getCounterName(counterId[i]), counterName[i]);
    }
}

GTEST_TEST(counterManagerBasicTest, move)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i], 5);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 5);

        counterManager_int.pegCounter(counterId[i], 111);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 116);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i], 5.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 5.0);

        counterManager_double.pegCounter(counterId[i], 111.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 116.0);
    }

    CounterManager<int> intCopy(std::move(counterManager_int));
    for (size_t i = 0; i < arrSize; ++i)
    {
        EXPECT_EQ(intCopy.getCounter(counterId[i]), std::make_pair(counterName[i], 116));
        EXPECT_EQ(intCopy.getCounterValue(counterId[i]), 116);
        EXPECT_EQ(intCopy.getCounterName(counterId[i]), counterName[i]);
    }

    CounterManager<int> intCopy2;
    intCopy2 = std::move(intCopy);

    for (size_t i = 0; i < arrSize; ++i)
    {
        EXPECT_EQ(intCopy2.getCounter(counterId[i]), std::make_pair(counterName[i], 116));
        EXPECT_EQ(intCopy2.getCounterValue(counterId[i]), 116);
        EXPECT_EQ(intCopy2.getCounterName(counterId[i]), counterName[i]);
    }
}

GTEST_TEST(counterManagerBasicTest, resetCounter)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i], 5);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 5);

        counterManager_int.pegCounter(counterId[i], 111);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 116);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i], 5.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 5.0);

        counterManager_double.pegCounter(counterId[i], 111.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 116.0);
    }

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.resetCounter(counterId[i]);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);

        counterManager_double.resetCounter(counterId[i]);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
    }
}

GTEST_TEST(counterManagerBasicTest, resetAllCounters)
{
    constexpr size_t arrSize = 7;
    Counters::counterId_t counterId[arrSize] = {1, 2, 3, 5, 11, 17, 41};
    std::string counterName[arrSize] = {"name1", "name2", "name3", "name5", "name11", "name17", "name41"};

    CounterManager<int> counterManager_int;
    CounterManager<double> counterManager_double;

    for (size_t i = 0; i < arrSize; ++i)
    {
        counterManager_int.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_int.getCounter(counterId[i]), std::make_pair(counterName[i], 0));
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_EQ(counterManager_int.getCounterName(counterId[i]), counterName[i]);

        counterManager_int.pegCounter(counterId[i], 5);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 5);

        counterManager_int.pegCounter(counterId[i], 111);
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 116);

        counterManager_double.addCounter(counterId[i], counterName[i]);
        EXPECT_EQ(counterManager_double.getCounter(counterId[i]), std::make_pair(counterName[i], 0.0));
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
        EXPECT_EQ(counterManager_double.getCounterName(counterId[i]), counterName[i]);

        counterManager_double.pegCounter(counterId[i], 5.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 5.0);

        counterManager_double.pegCounter(counterId[i], 111.0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 116.0);
    }

    counterManager_int.resetAllCounters();
    counterManager_double.resetAllCounters();

    for (size_t i = 0; i < arrSize; ++i)
    {
        EXPECT_EQ(counterManager_int.getCounterValue(counterId[i]), 0);
        EXPECT_DOUBLE_EQ(counterManager_double.getCounterValue(counterId[i]), 0.0);
    }
}