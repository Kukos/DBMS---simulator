#include <workload/analyzer/analyzerStepQueryWearoutNormalized.hpp>
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
#include <index/dbIndexRawToColumnWrapper.hpp>
#include <index/dsm.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(analyzerStepQueryWearoutNormalizedTestRaw, interface)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;

    PhantomIndex* ph = new PhantomIndex(disk2, true);
    DBIndex* index2 = ph;

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

    WorkloadAnalyzerStepQueryWearoutNormalized analyzer;
    EXPECT_EQ(analyzer.getName(), std::string("QueryWearoutNormalized"));
    EXPECT_NE(analyzer.analyzeWorkloadCounters(w), std::string(""));

    delete index;
    delete index2;
}

GTEST_TEST(analyzerStepQueryWearoutNormalizedTestRaw, analyze)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    BPTree* bp = new BPTree(disk, 8, 64, 1 << 14, true);
    DBIndex* index = bp;

    PhantomIndex* ph = new PhantomIndex(disk2, true);
    DBIndex* index2 = ph;

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

    WorkloadAnalyzerStepQueryWearoutNormalized analyzer;
    EXPECT_EQ(analyzer.analyzeWorkloadCounters(w), std::string("Type\tB+Tree\tPhantomIndex\n1\t1.000000\t0.000000\n2\t1.000000\t0.000000\n3\t1.000000\t0.000000\n4\t0.000000\t0.000000\n5\t0.000000\t0.000000\n"));

    delete index;
    delete index2;
}

GTEST_TEST(analyzerStepQueryWearoutNormalizedTestColumn, interface)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    DSM* dsm = new DSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = dsm;
    index->insertEntries(1000);

    PhantomIndex* ph = new PhantomIndex(disk2, true);
    DBIndexColumn* index2 = new DBIndexRawToColumnWrapper(ph, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndexColumn*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(200));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100), std::vector<size_t>{0, 2, 3, 4}));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), std::vector<size_t>{0, 2, 3, 4}, 100);
    w.addStep(step);
    steps.push_back(step);

    EXPECT_EQ(w.getNumIndexes(), 2);
    EXPECT_EQ(w.getNumSteps(), 5);

    EXPECT_EQ(w.getAllTotalCounters().size(), 0);
    EXPECT_EQ(w.getAllStepCounters().size(), 0);

    w.run();

    WorkloadAnalyzerStepQueryWearoutNormalized analyzer;
    EXPECT_EQ(analyzer.getName(), std::string("QueryWearoutNormalized"));
    EXPECT_NE(analyzer.analyzeWorkloadCounters(w), std::string(""));

    delete index;
    delete index2;
}

GTEST_TEST(analyzerStepQueryWearoutNormalizedTestColumn, analyze)
{
    Disk* disk = new DiskSSD_Samsung840();
    Disk* disk2 = new DiskSSD_Samsung840();

    DSM* dsm = new DSM(disk, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    DBIndexColumn* index = dsm;
    index->insertEntries(1000);

    PhantomIndex* ph = new PhantomIndex(disk2, true);
    DBIndexColumn* index2 = new DBIndexRawToColumnWrapper(ph, std::vector<size_t>{8, 16, 32, 4, 4, 8});
    index2->insertEntries(1000);

    std::vector<WorkloadStep*> steps;
    std::vector<DBIndexColumn*> indexes;

    indexes.push_back(index);

    steps.push_back(new WorkloadStepInsert(100));
    steps.push_back(new WorkloadStepBulkload(200));
    steps.push_back(new WorkloadStepDelete(200));
    steps.push_back(new WorkloadStepPSearch(static_cast<size_t>(100), std::vector<size_t>{0, 2, 3, 4}));

    Workload w(indexes, steps);

    w.addIndex(index2);
    indexes.push_back(index2);

    WorkloadStep* step = new WorkloadStepRSearch(static_cast<size_t>(10), std::vector<size_t>{0, 2, 3, 4}, 100);
    w.addStep(step);
    steps.push_back(step);

    EXPECT_EQ(w.getNumIndexes(), 2);
    EXPECT_EQ(w.getNumSteps(), 5);

    EXPECT_EQ(w.getAllTotalCounters().size(), 0);
    EXPECT_EQ(w.getAllStepCounters().size(), 0);

    w.run();

    WorkloadAnalyzerStepQueryWearoutNormalized analyzer;
    EXPECT_EQ(analyzer.analyzeWorkloadCounters(w), std::string("Type\tDSM\tPhantomIndex\n1\t1.000000\t0.000000\n2\t1.000000\t0.000000\n3\t1.000000\t0.000000\n4\t0.000000\t0.000000\n5\t0.000000\t0.000000\n"));

    delete index;
    delete index2;
}