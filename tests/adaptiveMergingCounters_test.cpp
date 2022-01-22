#include <observability/adaptiveMergingCounters.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(adaptiveMergingCountersBasicTest, interface)
{
    AdaptiveMergingCounters adaptiveMergingCounters;


    // double counters
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), 0.0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME), 0.0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME), std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME), 0.0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME), std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME), 0.0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME), std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 0.0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), std::string("ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME"), 0.0));

    // long counters
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS), std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(adaptiveMergingCounters.getCounterName(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), std::string("ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS"));
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), std::make_pair(std::string("ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS"), 0L));

}

GTEST_TEST(adaptiveMergingCountersBasicTest, operatorsDouble)
{
    enum AdaptiveMergingCounters::AdaptiveMergingCountersD e = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME;

    EXPECT_EQ(e++, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME);

    EXPECT_EQ(++e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME);

    EXPECT_EQ(e--, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME);

    EXPECT_EQ(--e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
}

GTEST_TEST(adaptiveMergingCountersBasicTest, operatorsLong)
{
    enum AdaptiveMergingCounters::AdaptiveMergingCountersL e = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS;

    EXPECT_EQ(e++, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);

    EXPECT_EQ(++e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);

    EXPECT_EQ(e--, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);

    EXPECT_EQ(--e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
    EXPECT_EQ(e, AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
}

GTEST_TEST(adaptiveMergingCountersBasicTest, pegOnlyDouble)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const double values[] = {1.0, 2.0};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, values[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), values[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // check that RO AVG counters are still 0
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, 0.0);
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);
}

GTEST_TEST(adaptiveMergingCountersBasicTest, pegOnlyLong)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const long values[] = {41, 17};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, values[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), values[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);
}

GTEST_TEST(adaptiveMergingCountersBasicTest, pegAll)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const long valuesL[] = {41, 17};
    const double valuesD[] = {1.0, 2.0};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(adaptiveMergingCountersBasicTest, copy)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const long valuesL[] = {41, 17};
    const double valuesD[] = {1.0, 2.0};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    AdaptiveMergingCounters copy(adaptiveMergingCounters);
    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(copy.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);


    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    AdaptiveMergingCounters copy2;
    copy2 = copy;
    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy2.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(copy2.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);


    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(adaptiveMergingCountersBasicTest, move)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const long valuesL[] = {41, 17};
    const double valuesD[] = {1.0, 2.0};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    AdaptiveMergingCounters copy(std::move(adaptiveMergingCounters));
    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(copy.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);


    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    AdaptiveMergingCounters copy2;
    copy2 = std::move(copy);
    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy2.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(copy2.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);


    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(adaptiveMergingCountersBasicTest, resetCounter)
{
    AdaptiveMergingCounters adaptiveMergingCounters;

    const long valuesL[] = {41, 17};
    const double valuesD[] = {1.0, 2.0};

    size_t index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(adaptiveMergingCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17);
    EXPECT_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME; ++id)
    {
        adaptiveMergingCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0);

    index = 0;
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME; id <= AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    adaptiveMergingCounters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
    EXPECT_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), 0);

    adaptiveMergingCounters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME);
    EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), 0.0);

    adaptiveMergingCounters.resetAllCounters();
    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME; id < AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(adaptiveMergingCounters.getCounterValue(id), 0.0);

    for (auto id = AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS; id < AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(adaptiveMergingCounters.getCounterValue(id), 0);
}