#include <workload/workloadStepRSearch.hpp>
#include <disk/diskSSD.hpp>
#include <index/phantomIndex.hpp>
#include <index/bptree.hpp>
#include <index/fbdsm.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(workloadStepRSearchTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_NE(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTest, singleStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTest, singleStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    index->insertEntries(1000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, 2);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTest, multiStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTest, multiStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    index->insertEntries(1000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, 2);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);

    WorkloadStepRSearch copy(*dynamic_cast<WorkloadStepRSearch*>(w));
    EXPECT_EQ(copy.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    WorkloadStepRSearch copy2(copy);
    EXPECT_EQ(copy2.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    WorkloadStepRSearch copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}


GTEST_TEST(workloadStepRSearchTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);

    WorkloadStepRSearch copy(std::move(*dynamic_cast<WorkloadStepRSearch*>(w)));
    EXPECT_EQ(copy.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    WorkloadStepRSearch copy2(std::move(copy));
    EXPECT_EQ(copy2.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    WorkloadStepRSearch copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealRaw, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_NE(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealRaw, singleStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealRaw, singleStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, 2);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealRaw, multiStep)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), 100);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealRaw, multiStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;
    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, 2);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealColumn, interface)
{
    Disk* disk = new DiskSSD_Samsung840();
    FBDSM* fbdsm = new FBDSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = fbdsm;

    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), std::vector<size_t>{0, 2, 3, 4}, 100);

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

    EXPECT_NE(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealColumn, singleStep)
{
    Disk* disk = new DiskSSD_Samsung840();
    FBDSM* fbdsm = new FBDSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = fbdsm;

    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), std::vector<size_t>{0, 2, 3, 4}, 100);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealColumn, singleStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();
    FBDSM* fbdsm = new FBDSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = fbdsm;

    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, std::vector<size_t>{0, 2, 3, 4}, 2);

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

    EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealColumn, multiStep)
{
    Disk* disk = new DiskSSD_Samsung840();
    FBDSM* fbdsm = new FBDSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = fbdsm;

    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, static_cast<size_t>(1000), std::vector<size_t>{0, 2, 3, 4}, 100);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100L);
    }

    delete w;
    delete index;
}

GTEST_TEST(workloadStepRSearchTestRealColumn, multiStepSel)
{
    Disk* disk = new DiskSSD_Samsung840();
    FBDSM* fbdsm = new FBDSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = fbdsm;

    index->bulkloadEntries(10000);

    WorkloadStep* w = new WorkloadStepRSearch(index, 0.1, std::vector<size_t>{0, 2, 3, 4}, 2);

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

        EXPECT_EQ(w->getCounters().getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 2L);
    }

    delete w;
    delete index;
}