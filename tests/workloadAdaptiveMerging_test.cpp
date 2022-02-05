#include <workload/workloadAdaptiveMerging.hpp>
#include <disk/diskSSD.hpp>
#include <adaptiveMerging/fAdaptiveMerging.hpp>
#include <adaptiveMerging/adaptiveMerging.hpp>
#include <index/bptree.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(workloadAMTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const double sel = 0.05;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index2 = new BPTree(disk2, keySize, dataSize, nodeSize);

    DBIndex* am = new FAdaptiveMerging(index, startingEntries, partitionSize);
    DBIndex* am2 = new AdaptiveMerging(index2, startingEntries, partitionSize);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(am);
    indexes.push_back(am2);

    Workload* w = new WorkloadAdaptiveMerging(indexes, sel);

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 0);

    EXPECT_EQ(w->getAllTotalCounters().size(), 0);
    EXPECT_EQ(w->getAllStepCounters().size(), 0);

    w->run();

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 45);

    EXPECT_EQ(w->getAllTotalCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(w->getAllStepCounters()[1].size(), 45);


    delete w;
    delete am;
    delete am2;
}


GTEST_TEST(workloadAMTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const double sel = 0.05;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index2 = new BPTree(disk2, keySize, dataSize, nodeSize);

    DBIndex* am = new FAdaptiveMerging(index, startingEntries, partitionSize);
    DBIndex* am2 = new AdaptiveMerging(index2, startingEntries, partitionSize);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(am);
    indexes.push_back(am2);

    Workload* w = new WorkloadAdaptiveMerging(indexes, sel);

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 0);

    EXPECT_EQ(w->getAllTotalCounters().size(), 0);
    EXPECT_EQ(w->getAllStepCounters().size(), 0);

    w->run();

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 45);

    EXPECT_EQ(w->getAllTotalCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(w->getAllStepCounters()[1].size(), 45);


    WorkloadAdaptiveMerging* copy = new WorkloadAdaptiveMerging(*dynamic_cast<WorkloadAdaptiveMerging*>(w));
    EXPECT_EQ(copy->getNumIndexes(), 2);
    EXPECT_EQ(copy->getNumSteps(), 45);

    EXPECT_EQ(copy->getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy->getAllStepCounters().size(), 2);
    EXPECT_EQ(copy->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy->getAllStepCounters()[1].size(), 45);

    WorkloadAdaptiveMerging copy2(*copy);
    EXPECT_EQ(copy2.getNumIndexes(), 2);
    EXPECT_EQ(copy2.getNumSteps(), 45);

    EXPECT_EQ(copy2.getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy2.getAllStepCounters().size(), 2);
    EXPECT_EQ(copy2.getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy2.getAllStepCounters()[1].size(), 45);

    WorkloadAdaptiveMerging copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getNumIndexes(), 2);
    EXPECT_EQ(copy3.getNumSteps(), 45);

    EXPECT_EQ(copy3.getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy3.getAllStepCounters().size(), 2);
    EXPECT_EQ(copy3.getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy3.getAllStepCounters()[1].size(), 45);

    delete w;
    delete copy;
    delete am;
    delete am2;
}

GTEST_TEST(workloadAMTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const double sel = 0.05;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index2 = new BPTree(disk2, keySize, dataSize, nodeSize);

    DBIndex* am = new FAdaptiveMerging(index, startingEntries, partitionSize);
    DBIndex* am2 = new AdaptiveMerging(index2, startingEntries, partitionSize);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(am);
    indexes.push_back(am2);

    Workload* w = new WorkloadAdaptiveMerging(indexes, sel);

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 0);

    EXPECT_EQ(w->getAllTotalCounters().size(), 0);
    EXPECT_EQ(w->getAllStepCounters().size(), 0);

    w->run();

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 45);

    EXPECT_EQ(w->getAllTotalCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(w->getAllStepCounters()[1].size(), 45);


    WorkloadAdaptiveMerging* copy = new WorkloadAdaptiveMerging(std::move(*dynamic_cast<WorkloadAdaptiveMerging*>(w)));
    EXPECT_EQ(copy->getNumIndexes(), 2);
    EXPECT_EQ(copy->getNumSteps(), 45);

    EXPECT_EQ(copy->getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy->getAllStepCounters().size(), 2);
    EXPECT_EQ(copy->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy->getAllStepCounters()[1].size(), 45);

    WorkloadAdaptiveMerging copy2(std::move(*copy));
    EXPECT_EQ(copy2.getNumIndexes(), 2);
    EXPECT_EQ(copy2.getNumSteps(), 45);

    EXPECT_EQ(copy2.getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy2.getAllStepCounters().size(), 2);
    EXPECT_EQ(copy2.getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy2.getAllStepCounters()[1].size(), 45);

    WorkloadAdaptiveMerging copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getNumIndexes(), 2);
    EXPECT_EQ(copy3.getNumSteps(), 45);

    EXPECT_EQ(copy3.getAllTotalCounters().size(), 2);
    EXPECT_EQ(copy3.getAllStepCounters().size(), 2);
    EXPECT_EQ(copy3.getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(copy3.getAllStepCounters()[1].size(), 45);

    delete w;
    delete copy;
    delete am;
    delete am2;
}

GTEST_TEST(workloadAMTest, execute)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const double sel = 0.05;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index2 = new BPTree(disk2, keySize, dataSize, nodeSize);

    DBIndex* am = new FAdaptiveMerging(index, startingEntries, partitionSize);
    DBIndex* am2 = new AdaptiveMerging(index2, startingEntries, partitionSize);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndex*> indexes;

    indexes.push_back(am);
    indexes.push_back(am2);

    Workload* w = new WorkloadAdaptiveMerging(indexes, sel);

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 0);

    EXPECT_EQ(w->getAllTotalCounters().size(), 0);
    EXPECT_EQ(w->getAllStepCounters().size(), 0);

    w->run();

    EXPECT_EQ(w->getNumIndexes(), 2);
    EXPECT_EQ(w->getNumSteps(), 45);

    EXPECT_EQ(w->getAllTotalCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters().size(), 2);
    EXPECT_EQ(w->getAllStepCounters()[0].size(), 45);
    EXPECT_EQ(w->getAllStepCounters()[1].size(), 45);

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME), 0.0);

        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_TIME), 0.0);

        EXPECT_DOUBLE_EQ(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_TIME),
                                                                w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME) +
                                                                w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME));

        EXPECT_DOUBLE_EQ(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME),
                                                                w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_TIME) +
                                                                w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME) +
                                                                w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME));

        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS), 0.0);

        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS), 0.0);
        EXPECT_GT(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS), 0.0);

        EXPECT_EQ(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS),
                                                         w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS) +
                                                         w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS));

        EXPECT_EQ(w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS),
                                                         w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS) +
                                                         w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS) +
                                                         w->getTotalCounters(i).getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS));
    }


    delete w;
    delete am;
    delete am2;
}
