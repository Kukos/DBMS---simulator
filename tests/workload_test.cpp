#include <workload/workload.hpp>
#include <workload/workloadStep.hpp>
#include <workload/workloadStepInsert.hpp>
#include <workload/workloadStepBulkload.hpp>
#include <workload/workloadStepDelete.hpp>
#include <workload/workloadStepPSearch.hpp>
#include <workload/workloadStepRSearch.hpp>
#include <disk/diskSSD.hpp>
#include <index/phantomIndex.hpp>
#include <index/bptree.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(workloadTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(200));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100)));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), 100);
    w.addStep(step);
    steps.push_back(step);

    EXPECT_EQ(w.getNumIndexes(), 2);
    EXPECT_EQ(w.getNumSteps(), 5);

    EXPECT_EQ(w.getAllTotalCounters().size(), 0);
    EXPECT_EQ(w.getAllStepCounters().size(), 0);

    w.run();

    EXPECT_EQ(w.getNumIndexes(), 2);
    EXPECT_EQ(w.getNumSteps(), 5);

    EXPECT_EQ(w.getAllTotalCounters().size(), 2);
    EXPECT_EQ(w.getAllStepCounters().size(), 2);
    EXPECT_EQ(w.getAllStepCounters()[0].size(), 5);
    EXPECT_EQ(w.getAllStepCounters()[1].size(), 5);

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, singleStepInsert)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);


    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    Workload w(indexes, steps);

    w.addIndex(index2);

    w.run();

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);


    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));

    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));


    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, singleStepBulkload)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepBulkload(100));
    Workload w(indexes, steps);

    w.addIndex(index2);

    w.run();

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);


    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));

    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));


    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, singleStepDelete)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepDelete(100));
    Workload w(indexes, steps);

    w.addIndex(index2);

    w.run();

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);


    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));

    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));


    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, singleStepPSearch)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);


    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100)));
    Workload w(indexes, steps);

    w.addIndex(index2);

    w.run();

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);


    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));

    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));


    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, singleStepRSearch)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);


    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepRSearch(static_cast<size_t>(50), 100));
    Workload w(indexes, steps);

    w.addIndex(index2);

    w.run();

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);


    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));

    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS));


    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, multiSteps)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(100));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100)));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), 100);
    w.addStep(step);
    steps.push_back(step);

    w.run();

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(100));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100)));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), 100);
    w.addStep(step);
    steps.push_back(step);

    w.run();

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    Workload copy(w);

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(copy.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(copy.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(copy.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(copy.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(copy.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(copy.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    Workload copy2;
    copy2 = copy;

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(copy2.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(copy2.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(copy2.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(copy2.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(copy2.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(copy2.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    delete index;
    delete index2;
}

GTEST_TEST(workloadTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;
    index->insertEntries(1000);

    BPTree* bp = new BPTree(disk2, 8, 64, 1 << 14, true);
    DBIndex* index2 = bp;
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(100));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100)));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), 100);
    w.addStep(step);
    steps.push_back(step);

    w.run();

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(w.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(w.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(w.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(w.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(w.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(w.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(w.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(w.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(w.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    Workload copy(std::move(w));

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(copy.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(copy.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(copy.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(copy.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(copy.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(copy.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(copy.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(copy.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(copy.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(copy.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(copy.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    Workload copy2;
    copy2 = std::move(copy);

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        {
            if (i == 0)
            {
                EXPECT_DOUBLE_EQ(copy2.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_DOUBLE_EQ(copy2.getTotalCounters(i).getCounter(id).second, 0.0);
            }
            else
            {
                EXPECT_NE(copy2.getTotalCounters(i).getCounterValue(id), 0.0);
                EXPECT_NE(copy2.getTotalCounters(i).getCounter(id).second, 0.0);
            }
        }

        for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        {
            if (i == 0 && (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 0L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if ((id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT || id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT))
            {
                EXPECT_NE(copy2.getTotalCounters(i).getCounterValue(id), 0);
                EXPECT_NE(copy2.getTotalCounters(i).getCounter(id).second, 0L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 1L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 1L);
            }
            else if (id == WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 401L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 401L);
            }
            else
            {
                EXPECT_EQ(copy2.getTotalCounters(i).getCounterValue(id), 100L);
                EXPECT_EQ(copy2.getTotalCounters(i).getCounter(id).second, 100L);
            }
        }
    }

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME),copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));

    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_DOUBLE_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


    EXPECT_NE(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME), copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME),copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME), copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_NE(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
    EXPECT_NEAR(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.001);

    EXPECT_EQ(copy2.getStepCounters(0, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 0).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);
    EXPECT_EQ(copy2.getStepCounters(1, 1).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS), 1);

    EXPECT_EQ(copy2.getStepCounters(0, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 2).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 3).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS), 100);

    EXPECT_EQ(copy2.getStepCounters(0, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);
    EXPECT_EQ(copy2.getStepCounters(1, 4).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 100);

    delete index;
    delete index2;
}