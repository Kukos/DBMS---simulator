#include <logger/logger.hpp>
#include <table/dbTable.hpp>
#include <table/dbTableTPCC.hpp>
#include <disk/diskSSD.hpp>
#include <workload/workloadLib.hpp>
#include <index/falsmtree.hpp>
#include <index/lsmtree.hpp>
#include <threadPool/dbThreadPool.hpp>

#include <random>
#include <iostream>
#include <numeric>
#include <fstream>
#include <filesystem>

#define FALSM_SANDBOX_DIRECTORY_PATH          "../experimentResults/phd/falsm/sandbox"
#define FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH "../experimentResults/phd/falsm/real"

#define MY_LUCKY_SEED 235111741 // euler lucky numbers 2, 3, 5, 11, 7, 41

[[maybe_unused]] static void ex_phd_basic_bulkloadAndRSearchRandomSel_step(const std::string& exName, Disk* disk, DBTable* table, size_t startingEntriesInIndex, size_t entriesToInsert, size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    std::vector<std::string> names {"1", "2", "3", "4", "5", "10", "15", "20", "25", "50", "LSM"};

    DBIndex* falsm1 = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 1);
    DBIndex* falsm2 = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 2);
    DBIndex* falsm3 = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 3);
    DBIndex* falsm4 = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 4);
    DBIndex* falsm5 = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 5);
    DBIndex* falsm10 = new FALSMTree(names[5].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 10);
    DBIndex* falsm15 = new FALSMTree(names[6].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 15);
    DBIndex* falsm20 = new FALSMTree(names[7].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 20);
    DBIndex* falsm25 = new FALSMTree(names[8].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 25);
    DBIndex* falsm50 = new FALSMTree(names[9].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, 50);
    DBIndex* lsmClassic = new LSMTree(names[10].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);

    // std::string label = std::string("Bacic FALSM PHD ") + exName + std::string(" :") +
    //                     std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
    //                     std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
    //                     std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
    //                     std::string(" rsearches = ") + std::to_string(rsearches) +
    //                     std::string(" sel = ") + std::to_string(sel) +
    //                     std::string(" indexesParams = {") +
    //                     std::string(" bufferSize = ") + std::to_string(bufferSize) +
    //                     std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
    //                     std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic};
    if (startingEntriesInIndex > 0)
    {
        for (size_t i = 0; i < indexes.size(); ++i)
            indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

        LOGGER_LOG_INFO("PHD {}: Topology created with {} entries", exName, startingEntriesInIndex);
    }

    std::vector<WorkloadStep*> steps;

    std::mt19937 rng(MY_LUCKY_SEED);
    std::uniform_int_distribution<size_t> randomizer(minRandom, maxRandom);

    size_t entriesLeft = entriesToInsert;
    while (entriesLeft > 0)
    {
        size_t n = std::min(entriesLeft, randomizer(rng));
        steps.push_back(new WorkloadStepBulkload(n));

        entriesLeft -= n;

        steps.push_back(new WorkloadStepRSearch(sel, rsearches));
    }

    Workload workload(indexes, steps);
    workload.run();

    std::string toPrint("");

    toPrint += std::string("T\tCzas wstawiania\tCzas wyszukiwania\tCałkowity czas\tCałkowity czas LSM\n");

    const std::vector<WorkloadCounters>& totalCounters = workload.getAllTotalCounters();
    double lsmTotalTime = totalCounters[names.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME);
    for (size_t i = 0; i < names.size() - 1; ++i) // last is lsmtree
    {
        toPrint += names[i]; // 1 or 2 or  .... this is T parameter

        toPrint += std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME)) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) +
                   std::string("\t") + std::to_string(lsmTotalTime) +
                   std::string("\n");
    }

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path);

    outfile << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete disk;
    delete table;

    for (size_t i = 0; i < indexes.size(); ++i)
        delete indexes[i];

    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex_phd_extendend_zd_tparam_step(const std::string& exName, Disk* disk, const DBTable* table)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t series;
        size_t qinsert;
        size_t qrsearch;
        double sel;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t series, size_t qinsert, size_t qrsearch, double sel, size_t qdelete)
        : name{name}, series{series}, qinsert{qinsert}, qrsearch{qrsearch}, sel{sel}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", series: " + std::to_string(series) +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qrsearch: " + std::to_string(qrsearch) +
                   ", sel: " + std::to_string(sel) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };
    const size_t startingEntries = 10000000; // 10m

    WorkloadExperiment wd = WorkloadExperiment(std::string("ZR_{D}"),
                                               10,
                                               1000000,
                                               40,
                                               0.01,
                                               10000);

    std::vector<WorkloadExperiment> wexperiments {wd};

    // std::string label = std::string("PHD LSM extendend T param ") + exName + std::string(" :") +
    //                     std::string(" workloads:[{") + "{" + wd.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"LSM-Tree", "FALSM-Tree (T=5)", "FALSM-Tree (T=10)", "FALSM-Tree (T=20)", "FALSM-Tree (T=40)"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* lsmClassic = new LSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* falsm5 = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 2);
        DBIndex* falsm10 = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 5);
        DBIndex* falsm20 = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 25);
        DBIndex* falsm40 = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 50);



        std::vector<DBIndex*> indexes {lsmClassic, falsm5, falsm10, falsm20, falsm40};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepBulkload(wexperiments[i].qinsert));
            steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, wexperiments[i].qrsearch));
            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
        }
        Workload workload(indexes, steps);

        for (size_t j = 0; j < indexes.size(); ++j)
            indexes[j]->createTopologyAfterInsert(startingEntries);

        LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];

        LOGGER_LOG_INFO("PHD {} Workload {} finished", exName, wexperiments[i].name);
    }

    // REAL time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // REAL memory wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex_phd_extendend_zd_sstable_step(const std::string& exName, Disk* disk, const DBTable* table)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t series;
        size_t qinsert;
        size_t qrsearch;
        double sel;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t series, size_t qinsert, size_t qrsearch, double sel, size_t qdelete)
        : name{name}, series{series}, qinsert{qinsert}, qrsearch{qrsearch}, sel{sel}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", series: " + std::to_string(series) +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qrsearch: " + std::to_string(qrsearch) +
                   ", sel: " + std::to_string(sel) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };
    const size_t startingEntries = 10000000; // 10m

    WorkloadExperiment wd = WorkloadExperiment(std::string("ZR_{D}"),
                                               10,
                                               1000000,
                                               10,
                                               0.01,
                                               10000);

    std::vector<WorkloadExperiment> wexperiments {wd};

    // std::string label = std::string("PHD LSM extendend SSTable ") + exName + std::string(" :") +
    //                     std::string(" workloads:[{") + "{" + wd.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"LSM-Tree", "FALSM-Tree (SST=1MB)", "FALSM-Tree (SST=2MB)", "FALSM-Tree (SST=4MB)", "FALSM-Tree (SST=8MB)"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* lsmClassic = new LSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* falsm1MB = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 20, 1 << 22, 5, 5);
        DBIndex* falsm2MB = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 22, 5, 5);
        DBIndex* falsm4MB = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 22, 1 << 22, 5, 5);
        DBIndex* falsm8MB = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 23, 1 << 22, 5, 5);



        std::vector<DBIndex*> indexes {lsmClassic, falsm1MB, falsm2MB, falsm4MB, falsm8MB};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepBulkload(wexperiments[i].qinsert));
            steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, wexperiments[i].qrsearch));
            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
        }
        Workload workload(indexes, steps);

        for (size_t j = 0; j < indexes.size(); ++j)
            indexes[j]->createTopologyAfterInsert(startingEntries);

        LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];

        LOGGER_LOG_INFO("PHD {} Workload {} finished", exName, wexperiments[i].name);
    }

    // REAL time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // REAL memory wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}


[[maybe_unused]] static void ex_phd_extendend_zd_buffer_step(const std::string& exName, Disk* disk, const DBTable* table)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t series;
        size_t qinsert;
        size_t qrsearch;
        double sel;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t series, size_t qinsert, size_t qrsearch, double sel, size_t qdelete)
        : name{name}, series{series}, qinsert{qinsert}, qrsearch{qrsearch}, sel{sel}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", series: " + std::to_string(series) +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qrsearch: " + std::to_string(qrsearch) +
                   ", sel: " + std::to_string(sel) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };
    const size_t startingEntries = 10000000; // 10m

    WorkloadExperiment wd = WorkloadExperiment(std::string("ZR_{D}"),
                                               10,
                                               1000000,
                                               10,
                                               0.01,
                                               10000);

    std::vector<WorkloadExperiment> wexperiments {wd};

    // std::string label = std::string("PHD LSM extendend BufferSize ") + exName + std::string(" :") +
    //                     std::string(" workloads:[{") + "{" + wd.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"LSM-Tree", "FALSM-Tree (BS=1MB)", "FALSM-Tree (BS=2MB)", "FALSM-Tree (BS=4MB)", "FALSM-Tree (BS=8MB)"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* lsmClassic = new LSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* falsm1MB = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 20, 5, 5);
        DBIndex* falsm2MB = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 5);
        DBIndex* falsm4MB = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 22, 5, 5);
        DBIndex* falsm8MB = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 23, 5, 5);

        std::vector<DBIndex*> indexes {lsmClassic, falsm1MB, falsm2MB, falsm4MB, falsm8MB};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepBulkload(wexperiments[i].qinsert));
            steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, wexperiments[i].qrsearch));
            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
        }
        Workload workload(indexes, steps);

        for (size_t j = 0; j < indexes.size(); ++j)
            indexes[j]->createTopologyAfterInsert(startingEntries);

        LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];

        LOGGER_LOG_INFO("PHD {} Workload {} finished", exName, wexperiments[i].name);
    }

    // REAL time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // REAL memory wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex_phd_extendend_step(const std::string& exName, Disk* disk, const DBTable* table)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t series;
        size_t qinsert;
        size_t qrsearch;
        double sel;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t series, size_t qinsert, size_t qrsearch, double sel, size_t qdelete)
        : name{name}, series{series}, qinsert{qinsert}, qrsearch{qrsearch}, sel{sel}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", series: " + std::to_string(series) +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qrsearch: " + std::to_string(qrsearch) +
                   ", sel: " + std::to_string(sel) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };
    const size_t startingEntries = 10000000; // 10m
    WorkloadExperiment wa = WorkloadExperiment(std::string("ZR_{A}"),
                                               100,
                                               5,
                                               10,
                                               0.01,
                                               5);

    WorkloadExperiment wb = WorkloadExperiment(std::string("ZR_{B}"),
                                               5,
                                               100000,
                                               5,
                                               0.01,
                                               100000);

    WorkloadExperiment wc = WorkloadExperiment(std::string("ZR_{C}"),
                                               10,
                                               10000000,
                                               20,
                                               0.01,
                                               1000000);

    WorkloadExperiment wd = WorkloadExperiment(std::string("ZR_{D}"),
                                               10,
                                               1000000,
                                               10,
                                               0.01,
                                               10000);

    std::vector<WorkloadExperiment> wexperiments {wa, wb, wc, wd};

    // std::string label = std::string("PHD LSM extendend ") + exName + std::string(" :") +
    //                     std::string(" workloads:[{") + wa.toString() + "}, {" + wb.toString()  + "}, {" + wc.toString()  + "}" + "}, {" + wd.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"LSM-Tree", "FALSM-Tree"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* lsmClassic = new LSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* falsm = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 21, 1 << 21, 5, 4);

        std::vector<DBIndex*> indexes {lsmClassic, falsm};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepBulkload(wexperiments[i].qinsert));
            steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, wexperiments[i].qrsearch));
            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
        }
        Workload workload(indexes, steps);

        for (size_t j = 0; j < indexes.size(); ++j)
            indexes[j]->createTopologyAfterInsert(startingEntries);

        LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];

        LOGGER_LOG_INFO("PHD {} Workload {} finished", exName, wexperiments[i].name);
    }

    // REAL time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to FALSM-Tree time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double falsmTime = std::max(1.0, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) / falsmTime) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_normalized") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    // REAL memory wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to FALSM-Tree wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double falsmwearout = std::max(1L, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / falsmwearout) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout_normalized") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex_phd_basic_batch(std::vector<std::future<bool>>& futures)
{
    {
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_20rsearches_samsung_warehouse", new DiskSSD_Samsung840(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_40rsearches_samsung_warehouse", new DiskSSD_Samsung840(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_100rsearches_samsung_warehouse", new DiskSSD_Samsung840(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_250rsearches_samsung_warehouse", new DiskSSD_Samsung840(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));
    }

    {
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_20rsearches_toshiba_warehouse", new DiskSSD_ToshibaVX500(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_40rsearches_toshiba_warehouse", new DiskSSD_ToshibaVX500(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_100rsearches_toshiba_warehouse", new DiskSSD_ToshibaVX500(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_250rsearches_toshiba_warehouse", new DiskSSD_ToshibaVX500(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));
    }

    {
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_20rsearches_intel_warehouse", new DiskSSD_IntelDCP4511(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_40rsearches_intel_warehouse", new DiskSSD_IntelDCP4511(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_100rsearches_intel_warehouse", new DiskSSD_IntelDCP4511(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_250rsearches_intel_warehouse", new DiskSSD_IntelDCP4511(), new DBTable_TPCC_Warehouse(), 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));
    }


    {
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_20rsearches_samsung_neworder", new DiskSSD_Samsung840(), new DBTable_TPCC_NewOrder(), 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_40rsearches_samsung_neworder", new DiskSSD_Samsung840(), new DBTable_TPCC_NewOrder(), 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_100rsearches_samsung_neworder", new DiskSSD_Samsung840(), new DBTable_TPCC_NewOrder(), 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_250rsearches_samsung_neworder", new DiskSSD_Samsung840(), new DBTable_TPCC_NewOrder(), 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));
    }

    {
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_20rsearches_samsung_customer", new DiskSSD_Samsung840(), new DBTable_TPCC_Customer(), 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_40rsearches_samsung_customer", new DiskSSD_Samsung840(), new DBTable_TPCC_Customer(), 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_100rsearches_samsung_customer", new DiskSSD_Samsung840(), new DBTable_TPCC_Customer(), 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_bulkloadAndRSearchRandomSel_step, "ex0_250rsearches_samsung_customer", new DiskSSD_Samsung840(), new DBTable_TPCC_Customer(), 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));
    }
}

[[maybe_unused]] static void ex_phd_extendend_tparam_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_tparam_step, "ex1_extendend_zd_tparam_samsung_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_tparam_step, "ex1_extendend_zd_tparam_toshiba_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_tparam_step, "ex1_extendend_zd_tparam_intel_tpcc_warehouse", disk, table));
    }
}

[[maybe_unused]] static void ex_phd_extendend_sstable_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_sstable_step, "ex2_extendend_zd_sstable_samsung_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_sstable_step, "ex2_extendend_zd_sstable_toshiba_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_sstable_step, "ex2_extendend_zd_sstable_intel_tpcc_warehouse", disk, table));
    }
}

[[maybe_unused]] static void ex_phd_extendend_buffer_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_buffer_step, "ex3_extendend_zd_buffer_samsung_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_buffer_step, "ex3_extendend_zd_buffer_toshiba_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_zd_buffer_step, "ex3_extendend_zd_buffer_intel_tpcc_warehouse", disk, table));
    }
}

[[maybe_unused]] static void ex_phd_extendend_batch(std::vector<std::future<bool>>& futures)
{
    // {
    //     DBTable* table = new DBTable_TPCC_Warehouse();
    //     Disk* disk = new DiskSSD_Samsung840();
    //     futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex4_extendend_samsung_tpcc_warehouse", disk, table));
    // }

    // {
    //     DBTable* table = new DBTable_TPCC_Warehouse();
    //     Disk* disk = new DiskSSD_ToshibaVX500();
    //     futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex4_extendend_toshiba_tpcc_warehouse", disk, table));
    // }

    // {
    //     DBTable* table = new DBTable_TPCC_Warehouse();
    //     Disk* disk = new DiskSSD_IntelDCP4511();
    //     futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex4_extendend_intel_tpcc_warehouse", disk, table));
    // }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex4_extendend_samsung_tpcc_neworder", disk, table));
    }

     {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex4_extendend_samsung_tpcc_customer", disk, table));
    }
}


/**
 *
 * @brief Main PhD experiment functions for FALSM Tree
 *        By calling this function you will execute exeriments from PhD Thesis
 *
 */
void experimentPhdFALSMTree()
{
    LOGGER_LOG_INFO("Starting FALSMTree PHD experiments");

    std::filesystem::create_directories(FALSM_REAL_EXPERIMENTS_DIRECTORY_PATH);

    std::vector<std::future<bool>> futures;

    // ex_phd_basic_batch(futures);
    // ex_phd_extendend_tparam_batch(futures);
    // ex_phd_extendend_sstable_batch(futures);
    // ex_phd_extendend_buffer_batch(futures);
    // ex_phd_basic_batch(futures);
    ex_phd_extendend_batch(futures);

    LOGGER_LOG_INFO("FALSM PHD experiments {} job created", futures.size());
    for (size_t i = 0; i < futures.size(); ++i)
    {
        bool isComplete = futures[i].get();
        (void) isComplete;
    }

    LOGGER_LOG_INFO("Finished {} tasks from FALSM PHD experiments", futures.size());
}