#include <workload/workloadStepInsert.hpp>
#include <disk/diskSSD.hpp>
#include <index/phantomIndex.hpp>
#include <index/bptree.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(workloadStepInsertTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_NE(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTest, singleStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTest, multiStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        w->executeStep();
        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy(*dynamic_cast<WorkloadStepInsert*>(w));
    EXPECT_EQ(copy.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy2(copy);
    EXPECT_EQ(copy2.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy(std::move(*dynamic_cast<WorkloadStepInsert*>(w)));
    EXPECT_EQ(copy.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy2(std::move(copy));
    EXPECT_EQ(copy2.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    WorkloadStepInsert copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTestReal, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_NE(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTestReal, singleStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    w->executeStep();

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepInsertTestReal, multiStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepInsert(index, 100);

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
    {
        EXPECT_DOUBLE_EQ(w->getCounters().getCounterValue(id), 0.0);
        EXPECT_DOUBLE_EQ(w->getCounters().getCounter(id).second, 0.0);
    }

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
    {
        EXPECT_EQ(w->getCounters().getCounterValue(id), 0L);
        EXPECT_EQ(w->getCounters().getCounter(id).second, 0L);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        w->executeStep();
        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    }

    delete w;
    delete index;
}