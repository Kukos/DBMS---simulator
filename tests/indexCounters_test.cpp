#include <observability/indexCounters.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(indexCountersBasicTest, interface)
{
    IndexCounters indexCounters;

    // double counters
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME), std::string("INDEX_COUNTER_RW_INSERT_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RW_INSERT_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME), std::string("INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME), std::string("INDEX_COUNTER_RW_DELETE_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RW_DELETE_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME), std::string("INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME), std::string("INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME), std::string("INDEX_COUNTER_RO_INSERT_AVG_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_INSERT_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME), std::string("INDEX_COUNTER_RO_BULKLOAD_AVG_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_BULKLOAD_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME), std::string("INDEX_COUNTER_RO_DELETE_AVG_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_DELETE_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME), std::string("INDEX_COUNTER_RO_PSEARCH_AVG_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_PSEARCH_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME), std::string("INDEX_COUNTER_RO_RSEARCH_AVG_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_RSEARCH_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 0.0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), std::string("INDEX_COUNTER_RO_TOTAL_TIME"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), std::make_pair(std::string("INDEX_COUNTER_RO_TOTAL_TIME"), 0.0));

    // long counters
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(indexCounters.getCounterName(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), std::string("INDEX_COUNTER_RO_TOTAL_OPERATIONS"));
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), std::make_pair(std::string("INDEX_COUNTER_RO_TOTAL_OPERATIONS"), 0L));
}

GTEST_TEST(indexCountersBasicTest, operatorsDouble)
{
    enum IndexCounters::IndexCountersD e = IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME;

    EXPECT_EQ(e++, IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME);

    EXPECT_EQ(++e, IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME);

    EXPECT_EQ(e--, IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME);

    EXPECT_EQ(--e, IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME);
}

GTEST_TEST(indexCountersBasicTest, operatorsLong)
{
    enum IndexCounters::IndexCountersL e = IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS;

    EXPECT_EQ(e++, IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(++e, IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(e--, IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(--e, IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
}

GTEST_TEST(indexCountersBasicTest, pegOnlyDouble)
{
    IndexCounters indexCounters;

    const double values[] = {1.0, 2.0, 3.0, 5.0, 11.0};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        indexCounters.pegCounter(id, values[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), values[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // check that RO AVG counters are still 0
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, 0.0);
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
}

GTEST_TEST(indexCountersBasicTest, pegOnlyLong)
{
    IndexCounters indexCounters;

    const long values[] = {41, 17, 11, 5, 3};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        indexCounters.pegCounter(id, values[index]);
        EXPECT_EQ(indexCounters.getCounterValue(id), values[index]);
        EXPECT_EQ(indexCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);
}

GTEST_TEST(indexCountersBasicTest, pegAll)
{
    IndexCounters indexCounters;

    const long valuesL[] = {41, 17, 11, 5, 3};
    const double valuesD[] = {1.0, 2.0, 3.0, 5.0, 11.0};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        indexCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(indexCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(indexCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        indexCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(indexCountersBasicTest, copy)
{
    IndexCounters indexCounters;

    const long valuesL[] = {41, 17, 11, 5, 3};
    const double valuesD[] = {1.0, 2.0, 3.0, 5.0, 11.0};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        indexCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(indexCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(indexCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        indexCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    IndexCounters copy(indexCounters);
    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(copy.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    IndexCounters copy2;
    copy2 = copy;
    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy2.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(indexCountersBasicTest, move)
{
    IndexCounters indexCounters;

    const long valuesL[] = {41, 17, 11, 5, 3};
    const double valuesD[] = {1.0, 2.0, 3.0, 5.0, 11.0};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        indexCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(indexCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(indexCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        indexCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    IndexCounters copy(std::move(indexCounters));
    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(copy.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    IndexCounters copy2;
    copy2 = std::move(copy);
    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(copy2.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }
}

GTEST_TEST(indexCountersBasicTest, resetCounter)
{
    IndexCounters indexCounters;

    const long valuesL[] = {41, 17, 11, 5, 3};
    const double valuesD[] = {1.0, 2.0, 3.0, 5.0, 11.0};

    size_t index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++id)
    {
        indexCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(indexCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(indexCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of array
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS), 41 + 17 + 11 + 5 + 3);
    EXPECT_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 41 + 17 + 11 + 5 + 3);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++id)
    {
        indexCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME), 1.0 + 2.0 + 3.0 + 5.0 + 11.0);
    EXPECT_DOUBLE_EQ(indexCounters.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 1.0 + 2.0 + 3.0 + 5.0 + 11.0);

    index = 0;
    for (auto id = IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME; id <= IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index]));
        EXPECT_DOUBLE_EQ(indexCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index]));
        ++index;
    }

    indexCounters.resetCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS);
    EXPECT_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0);

    indexCounters.resetCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME);
    EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);

    indexCounters.resetAllCounters();
    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(indexCounters.getCounterValue(id), 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(indexCounters.getCounterValue(id), 0);
}