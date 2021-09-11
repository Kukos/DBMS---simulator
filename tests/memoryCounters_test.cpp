#include <observability/memoryCounters.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(memoryCountersBasicTest, interface)
{
    MemoryCounters memCounters;

    // double counters
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME), std::string("MEMORY_COUNTER_RW_READ_TOTAL_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME), std::make_pair(std::string("MEMORY_COUNTER_RW_READ_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME), std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME), std::make_pair(std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME), std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME), std::make_pair(std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME), std::string("MEMORY_COUNTER_RO_READ_AVG_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME), std::make_pair(std::string("MEMORY_COUNTER_RO_READ_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME), std::string("MEMORY_COUNTER_RO_WRITE_AVG_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME), std::make_pair(std::string("MEMORY_COUNTER_RO_WRITE_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME), std::string("MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME), std::make_pair(std::string("MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES), std::string("MEMORY_COUNTER_RO_READ_AVG_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RO_READ_AVG_BYTES"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_BYTES), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_BYTES), std::string("MEMORY_COUNTER_RO_WRITE_AVG_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RO_WRITE_AVG_BYTES"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES), std::string("MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES"), 0.0));

    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 0.0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), std::string("MEMORY_COUNTER_RO_TOTAL_TIME"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), std::make_pair(std::string("MEMORY_COUNTER_RO_TOTAL_TIME"), 0.0));


    // long counters
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES), std::string("MEMORY_COUNTER_RW_READ_TOTAL_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RW_READ_TOTAL_BYTES"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES), std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES), std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS), std::string("MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS), std::make_pair(std::string("MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS), std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS), std::make_pair(std::string("MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS), std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS), std::make_pair(std::string("MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), std::string("MEMORY_COUNTER_RO_TOTAL_BYTES"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), std::make_pair(std::string("MEMORY_COUNTER_RO_TOTAL_BYTES"), 0L));

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(memCounters.getCounterName(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), std::string("MEMORY_COUNTER_RO_TOTAL_OPERATIONS"));
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), std::make_pair(std::string("MEMORY_COUNTER_RO_TOTAL_OPERATIONS"), 0L));
}

GTEST_TEST(memoryCountersBasicTest, operatorsDouble)
{
    enum MemoryCounters::MemoryCountersD e = MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME;

    EXPECT_EQ(e++, MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME);

    EXPECT_EQ(++e, MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES);

    EXPECT_EQ(e--, MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME);

    EXPECT_EQ(--e, MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RO_WRITE_AVG_TIME);
}

GTEST_TEST(memoryCountersBasicTest, operatorsLong)
{
    enum MemoryCounters::MemoryCountersL e = MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES;

    EXPECT_EQ(e++, MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS);

    EXPECT_EQ(++e, MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS);

    EXPECT_EQ(e--, MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS);

    EXPECT_EQ(--e, MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES);
    EXPECT_EQ(e, MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES);
}

GTEST_TEST(memoryCountersBasicTest, pegOnlyDouble)
{
    MemoryCounters memCounters;

    const double values[] = {1.0, 3.0, 5.0};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        memCounters.pegCounter(id, values[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), values[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // check that RO AVG counters are still 0
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, 0.0);
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);
}

GTEST_TEST(memoryCountersBasicTest, pegOnlyLong)
{
    MemoryCounters memCounters;

    const long values[] = {41, 17, 11, 5, 3, 2};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        memCounters.pegCounter(id, values[index]);
        EXPECT_EQ(memCounters.getCounterValue(id), values[index]);
        EXPECT_EQ(memCounters.getCounter(id).second, values[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);
}

GTEST_TEST(memoryCountersBasicTest, pegAll)
{
    MemoryCounters memCounters;

    const long valuesL[] = {41, 17, 11, 5, 3, 2};
    const double valuesD[] = {1.0, 3.0, 5.0};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        memCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(memCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(memCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        memCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }
}

GTEST_TEST(memoryCountersBasicTest, copy)
{
    MemoryCounters memCounters;

    const long valuesL[] = {41, 17, 11, 5, 3, 2};
    const double valuesD[] = {1.0, 3.0, 5.0};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        memCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(memCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(memCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        memCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    MemoryCounters copy(memCounters);
    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    MemoryCounters copy2;
    copy2 = copy;

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }
}

GTEST_TEST(memoryCountersBasicTest, move)
{
    MemoryCounters memCounters;

    const long valuesL[] = {41, 17, 11, 5, 3, 2};
    const double valuesD[] = {1.0, 3.0, 5.0};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        memCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(memCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(memCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        memCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    MemoryCounters copy(std::move(memCounters));
    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    MemoryCounters copy2;
    copy2 = std::move(copy);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(copy2.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(copy2.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }
}


GTEST_TEST(memoryCountersBasicTest, resetCounters)
{
    MemoryCounters memCounters;

    const long valuesL[] = {41, 17, 11, 5, 3, 2};
    const double valuesD[] = {1.0, 3.0, 5.0};

    size_t index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++id)
    {
        memCounters.pegCounter(id, valuesL[index]);
        EXPECT_EQ(memCounters.getCounterValue(id), valuesL[index]);
        EXPECT_EQ(memCounters.getCounter(id).second, valuesL[index]);
        ++index;
    }

    // total should be sum of subsets
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES), 41 + 17 + 11);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_BYTES).second, 41 + 17 + 11);

    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS), 5 + 3 + 2);
    EXPECT_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_OPERATIONS).second, 5 + 3 + 2);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++id)
    {
        memCounters.pegCounter(id, valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index]);
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index]);
        ++index;
    }

    // total time counter should be equals sum of values
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME), 1.0 + 3.0 + 5.0);
    EXPECT_DOUBLE_EQ(memCounters.getCounter(MemoryCounters::MEMORY_COUNTER_RO_TOTAL_TIME).second, 1.0 + 3.0 + 5.0);

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_TIME; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), valuesD[index] / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, valuesD[index] / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    index = 0;
    for (auto id = MemoryCounters::MEMORY_COUNTER_RO_READ_AVG_BYTES; id <= MemoryCounters::MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES; ++id)
    {
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        EXPECT_DOUBLE_EQ(memCounters.getCounter(id).second, static_cast<double>(valuesL[index]) / static_cast<double>(valuesL[index + 3]));
        ++index;
    }

    memCounters.resetCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES);
    EXPECT_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES), 0);

    memCounters.resetCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME);
    EXPECT_DOUBLE_EQ(memCounters.getCounterValue(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME), 0.0);

    memCounters.resetAllCounters();
    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(memCounters.getCounterValue(id), 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(memCounters.getCounterValue(id), 0);
}