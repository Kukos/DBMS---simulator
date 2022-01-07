#include <workload/analyzer/analyzer.hpp>
#include <workload/analyzer/analyzerConsole.hpp>
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

GTEST_TEST(analyzerConsoleTest, interface)
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

    // uncomment this to see example of print
    // w.run();

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefault();

    a->runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}

GTEST_TEST(analyzerConsoleTest, copy)
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

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefault();

    a->runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy (*dynamic_cast<WorkloadAnalyzerConsoleDefault*>(a));
    copy.runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy2 (copy);
    copy2.runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy3;
    copy3 = copy2;
    copy3.runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}

GTEST_TEST(analyzerConsoleTest, move)
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

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefault();

    a->runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy(std::move(*dynamic_cast<WorkloadAnalyzerConsoleDefault*>(a)));
    copy.runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy2(std::move(copy));
    copy2.runAnalyze(w);

    WorkloadAnalyzerConsoleDefault copy3;
    copy3 = std::move(copy2);
    copy3.runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}

GTEST_TEST(analyzerConsoleExtTest, interface)
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

    // uncomment this to see example of print
    // w.run();

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefaultExtendend();

    a->runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}

GTEST_TEST(analyzerConsoleExtTest, copy)
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

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefaultExtendend();

    a->runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy (*dynamic_cast<WorkloadAnalyzerConsoleDefaultExtendend*>(a));
    copy.runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy2 (copy);
    copy2.runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy3;
    copy3 = copy2;
    copy3.runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}

GTEST_TEST(analyzerConsoleExtTest, move)
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

    WorkloadAnalyzer* a = new WorkloadAnalyzerConsoleDefaultExtendend();

    a->runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy(std::move(*dynamic_cast<WorkloadAnalyzerConsoleDefaultExtendend*>(a)));
    copy.runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy2(std::move(copy));
    copy2.runAnalyze(w);

    WorkloadAnalyzerConsoleDefaultExtendend copy3;
    copy3 = std::move(copy2);
    copy3.runAnalyze(w);

    delete a;

    delete index;
    delete index2;
}