#include <observability/workloadCounters.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(workloadCountersBasicTest, interface)
{
    WorkloadCounters workloadCounters;

    // double counters
    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME), std::string("WORKLOAD_COUNTER_RW_INSERT_AVG_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_INSERT_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME), std::string("WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME), std::string("WORKLOAD_COUNTER_RW_DELETE_AVG_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_DELETE_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME), std::string("WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME), std::string("WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME"), 0.0));

    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), std::string("WORKLOAD_COUNTER_RW_TOTAL_TIME"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), std::make_pair(std::string("WORKLOAD_COUNTER_RW_TOTAL_TIME"), 0.0));

    // long counters
    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS), std::string("WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS), std::make_pair(std::string("WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT), std::string("WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT), std::make_pair(std::string("WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT"), 0L));

    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT), 0);
    EXPECT_EQ(workloadCounters.getCounterName(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT), std::string("WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT"));
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT), std::make_pair(std::string("WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT"), 0L));
}

GTEST_TEST(workloadCountersBasicTest, operatorsDouble)
{
    enum WorkloadCounters::WorkloadCountersD e = WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME;

    EXPECT_EQ(e++, WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME);

    EXPECT_EQ(++e, WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME);

    EXPECT_EQ(e--, WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME);

    EXPECT_EQ(--e, WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME);
}

GTEST_TEST(workloadCountersBasicTest, operatorsLong)
{
    enum WorkloadCounters::WorkloadCountersL e = WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS;

    EXPECT_EQ(e++, WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(++e, WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(e--, WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS);

    EXPECT_EQ(--e, WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
    EXPECT_EQ(e, WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
}

GTEST_TEST(workloadCountersBasicTest, pegOnlyDouble)
{
    WorkloadCounters workloadCounters;

    double val = 1.0;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, val);
        val += 0.5;
    }

    val = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), val);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, val);
        val += 0.5;
    }
}

GTEST_TEST(workloadCountersBasicTest, pegOnlyLong)
{
    WorkloadCounters workloadCounters;

    long val = 1;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, val);
        val += 2;
    }

    val = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), val);
        EXPECT_EQ(workloadCounters.getCounter(id).second, val);
        val += 2;
    }
}

GTEST_TEST(workloadCountersBasicTest, pegAll)
{
    WorkloadCounters workloadCounters;

    double valD = 1.0;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valD);
        valD += 0.5;
    }

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, valD);
        valD += 0.5;
    }

    long valL = 1;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valL);
        valL += 2;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), valL);
        EXPECT_EQ(workloadCounters.getCounter(id).second, valL);
        valL += 2;
    }
}

GTEST_TEST(workloadCountersBasicTest, copy)
{
    WorkloadCounters workloadCounters;

    double valD = 1.0;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valD);
        valD += 0.5;
    }

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, valD);
        valD += 0.5;
    }

    long valL = 1;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valL);
        valL += 2;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), valL);
        EXPECT_EQ(workloadCounters.getCounter(id).second, valL);
        valL += 2;
    }

    WorkloadCounters copy(workloadCounters);

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valD);
        valD += 0.5;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valL);
        EXPECT_EQ(copy.getCounter(id).second, valL);
        valL += 2;
    }

    WorkloadCounters copy2;
    copy2 = copy;

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valD);
        valD += 0.5;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valL);
        EXPECT_EQ(copy2.getCounter(id).second, valL);
        valL += 2;
    }
}

GTEST_TEST(workloadCountersBasicTest, move)
{
    WorkloadCounters workloadCounters;

    double valD = 1.0;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valD);
        valD += 0.5;
    }

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, valD);
        valD += 0.5;
    }

    long valL = 1;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valL);
        valL += 2;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), valL);
        EXPECT_EQ(workloadCounters.getCounter(id).second, valL);
        valL += 2;
    }

    WorkloadCounters copy(std::move(workloadCounters));

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(copy.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(copy.getCounter(id).second, valD);
        valD += 0.5;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(copy.getCounterValue(id), valL);
        EXPECT_EQ(copy.getCounter(id).second, valL);
        valL += 2;
    }

    WorkloadCounters copy2;
    copy2 = std::move(copy);

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(copy2.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, valD);
        valD += 0.5;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(copy2.getCounterValue(id), valL);
        EXPECT_EQ(copy2.getCounter(id).second, valL);
        valL += 2;
    }
}

GTEST_TEST(workloadCountersBasicTest, resetCounter)
{
    WorkloadCounters workloadCounters;

    double valD = 1.0;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valD);
        valD += 0.5;
    }

    valD = 1.0;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), valD);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, valD);
        valD += 0.5;
    }

    workloadCounters.resetCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME);
    EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    long valL = 1;

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        workloadCounters.pegCounter(id, valL);
        valL += 2;
    }

    valL = 1;
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), valL);
        EXPECT_EQ(workloadCounters.getCounter(id).second, valL);
        valL += 2;
    }

    workloadCounters.resetCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS);
    EXPECT_EQ(workloadCounters.getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0);
    EXPECT_EQ(workloadCounters.getCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    workloadCounters.resetAllCounters();
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(workloadCounters.getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(workloadCounters.getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(workloadCounters.getCounterValue(id), 0L);
        EXPECT_EQ(workloadCounters.getCounter(id).second, 0L);
    }
}