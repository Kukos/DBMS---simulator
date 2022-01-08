#include <logger/logger.hpp>
#include <table/dbTable.hpp>
#include <table/dbTableTPCC.hpp>
#include <disk/diskSSD.hpp>
#include <workload/workloadLib.hpp>
#include <index/lsmtree.hpp>
#include <index/falsmtree.hpp>
#include <threadPool/dbThreadPool.hpp>

#include <random>
#include <iostream>
#include <numeric>
#include <fstream>
#include <filesystem>

#define LSM_SANDBOX_DIRECTORY_PATH          "../experimentResults/lsmPaper/sandbox"
#define LSM_REAL_EXPERIMENTS_DIRECTORY_PATH "../experimentResults/lsmPaper/paper"

#define MY_LUCKY_SEED 235111741 // euler lucky numbers 2, 3, 5, 11, 7, 41

[[maybe_unused]] static void sandbox_bulkloadRandom(std::string exName, size_t startingEntriesInIndex, size_t entriesToInsert, size_t minRandom, size_t maxRandom, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    DBTable* table = new DBTable_TPCC_Warehouse();
    Disk* disk = new DiskSSD_Samsung840();

    DBIndex* falsm = new FALSMTree(disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio);
    DBIndex* lsmClassic = new LSMTree("LSMTree classic", disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
    DBIndex* lsmCurCap = new LSMTree("LSM curCap", disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY);
    DBIndex* lsmMaxCap = new LSMTree("LSM maxCap", disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload test started: {}", exName, label);

    std::vector<DBIndex*> indexes {falsm, lsmClassic, lsmCurCap, lsmMaxCap};
    if (startingEntriesInIndex > 0)
    {
        for (size_t i = 0; i < indexes.size(); ++i)
            indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

        LOGGER_LOG_INFO("{}: Bulkload test Topology created with {} entries", exName, startingEntriesInIndex);
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
    }

    Workload workload(indexes, steps);
    workload.run();

    for (size_t i = 0; i < indexes.size(); ++i)
        if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
            LOGGER_LOG_ERROR("{}: Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);

    // for (size_t i = 0; i < indexes.size(); ++i)
        // LOGGER_LOG_INFO("{}", indexes[i]->toStringFull(true));

    DBThreadPool::mutex.lock();

    WorkloadAnalyzer* analyzer = new WorkloadAnalyzerConsole(label.c_str(), std::vector<WorkloadAnalyzerStep*>{new WorkloadAnalyzerStepTotalTime()});
    analyzer->runAnalyze(workload);

    DBThreadPool::mutex.unlock();

    delete disk;
    delete table;

    delete falsm;
    delete lsmClassic;
    delete lsmCurCap;
    delete lsmMaxCap;

    delete analyzer;

    LOGGER_LOG_INFO("{}: Bulkload test finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadPackageSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>packageSize, size_t startingEntriesInIndex, size_t entriesToInsert, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    if (labels.size() != packageSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != packageSize.size() {}", exName, labels.size(), packageSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string bulkloadPackageSizeString =  std::string("{") + std::accumulate(std::begin(packageSize), std::end(packageSize), std::string(), buildStringFromVector) + std::string("}");


    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = ") + bulkloadPackageSizeString +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST started: {}", exName, label);

    std::vector<std::string> names {"FALSM", "LSMTree insert", "LSMTree bulkload"};

    std::vector<Workload> workloads;
    for (size_t j = 0; j < packageSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST {} / {} started", exName, j + 1, packageSize.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST {} / {} Topology created with {} entries", exName, j + 1, packageSize.size(), startingEntriesInIndex);
        }

        std::vector<WorkloadStep*> steps;

        size_t entriesLeft = entriesToInsert;
        while (entriesLeft > 0)
        {
            size_t n = std::min(entriesLeft, packageSize[j]);
            steps.push_back(new WorkloadStepBulkload(n));

            entriesLeft -= n;
        }

        Workload workload(indexes, steps);
        workload.run();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload packageSizeTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        delete falsm;
        delete lsmClassic;
        delete lsmMaxCap;
    }

    std::string toPrint("Testing bulkloadSizePackage\t");
    toPrint += label + std::string("\n\n");

    toPrint += std::string("BulkloadPackageSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;


    LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadSSTableSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>ssTableSize, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t bufferSize, size_t levelRatio)
{
    if (labels.size() != ssTableSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != ssTableSize.size() {}", exName, labels.size(), ssTableSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string SSTableSizeString =  std::string("{") + std::accumulate(std::begin(ssTableSize), std::end(ssTableSize), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + SSTableSizeString +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload ssTableTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < ssTableSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload ssTableTEST {} / {} started", exName, j + 1, ssTableSize.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload ssTableTEST {} / {} Topology created with {} entries", exName, j + 1, ssTableSize.size(), startingEntriesInIndex);
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
        }

        Workload workload(indexes, steps);
        workload.run();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload ssTableTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        delete falsm;
        delete lsmClassic;
        delete lsmMaxCap;
    }

    std::string toPrint("Testing SSTable\t");
    toPrint += label + std::string("\n\n");

    toPrint += std::string("SSTableSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload ssTableTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadBufferSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>bufferSize, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t ssTableSize, size_t levelRatio)
{
    if (labels.size() != bufferSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != bufferSize.size() {}", exName, labels.size(), bufferSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string bufferSizeString =  std::string("{") + std::accumulate(std::begin(bufferSize), std::end(bufferSize), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + bufferSizeString +
                        std::string(" ssTableSize = ") +  std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < bufferSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST {} / {} started", exName, j + 1, bufferSize.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST {} / {} Topology created with {} entries", exName, j + 1, bufferSize.size(), startingEntriesInIndex);
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
        }

        Workload workload(indexes, steps);
        workload.run();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload BufferSizeTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        delete falsm;
        delete lsmClassic;
        delete lsmMaxCap;
    }

    std::string toPrint("Testing bufferSize\t");

    toPrint += label + std::string("\n\n");

    toPrint += std::string("BufferSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }

    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadLevelRatio(std::string exName, std::vector<std::string> labels, std::vector<size_t>levelRatio, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t ssTableSize, size_t bufferSize)
{
    if (labels.size() != levelRatio.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != levelRatio.size() {}", exName, labels.size(), levelRatio.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string levelRatioString =  std::string("{") + std::accumulate(std::begin(levelRatio), std::end(levelRatio), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") +  std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") +  std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + levelRatioString + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < levelRatio.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST {} / {} started", exName, j + 1, levelRatio.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j]);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST {} / {} Topology created with {} entries", exName, j + 1, levelRatio.size(), startingEntriesInIndex);
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
        }

        Workload workload(indexes, steps);
        workload.run();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload leveRatioTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        delete falsm;
        delete lsmClassic;
        delete lsmMaxCap;
    }

    std::string toPrint("Testing levelRatio\t");
    toPrint += label + std::string("\n\n");

    toPrint += std::string("LevelRatio");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchRandomSel(std::string exName, size_t startingEntriesInIndex, size_t entriesToInsert, size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    DBTable* table = new DBTable_TPCC_Warehouse();
    Disk* disk = new DiskSSD_Samsung840();

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};

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
    DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload test started: {}", exName, label);

    std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
    if (startingEntriesInIndex > 0)
    {
        for (size_t i = 0; i < indexes.size(); ++i)
            indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

        LOGGER_LOG_INFO("{}: Bulkload test Topology created with {} entries", exName, startingEntriesInIndex);
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

    for (size_t i = 0; i < indexes.size(); ++i)
        if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
            LOGGER_LOG_ERROR("{}: Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


    std::string toPrint("Testing bulkload + rsearch\t");
    toPrint += label + std::string("\n\n");

    toPrint += std::string("BulkoadTime\tRSearchTime\tTotalTime\n");
    for (size_t i = 0; i < names.size(); ++i)
    {
        toPrint += names[i];
        const std::vector<WorkloadCounters>& totalCounters = workload.getAllTotalCounters();
        toPrint += (names[i] == std::string("LSMTree insert") ?  std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME))) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME)) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) +
                   std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete disk;
    delete table;

    for (size_t i = 0; i < indexes.size(); ++i)
        delete indexes[i];

    LOGGER_LOG_INFO("{}: Bulkload test finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchRandom(std::string exName, size_t startingEntriesInIndex, size_t entriesToInsert, size_t minRandom, size_t maxRandom, size_t rsearches, size_t entriesToFindPerSearch, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    DBTable* table = new DBTable_TPCC_Warehouse();
    Disk* disk = new DiskSSD_Samsung840();

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};

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
    DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" entriesToFindPerSearch = ") + std::to_string(entriesToFindPerSearch) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload test started: {}", exName, label);

    std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
    if (startingEntriesInIndex > 0)
    {
        for (size_t i = 0; i < indexes.size(); ++i)
            indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

        LOGGER_LOG_INFO("{}: Bulkload test Topology created with {} entries", exName, startingEntriesInIndex);
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

        steps.push_back(new WorkloadStepRSearch(entriesToFindPerSearch, rsearches));
    }

    Workload workload(indexes, steps);
    workload.run();

    for (size_t i = 0; i < indexes.size(); ++i)
        if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
            LOGGER_LOG_ERROR("{}: Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


    std::string toPrint("Testing bulkload + rsearch\t");
    toPrint += label + std::string("\n\n");

    toPrint += std::string("BulkoadTime\tRSearchTime\tTotalTime\n");
    for (size_t i = 0; i < names.size(); ++i)
    {
        toPrint += names[i];
        const std::vector<WorkloadCounters>& totalCounters = workload.getAllTotalCounters();
        toPrint += (names[i] == std::string("LSMTree insert") ?  std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME))) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME)) +
                   std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) +
                   std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete disk;
    delete table;

    for (size_t i = 0; i < indexes.size(); ++i)
        delete indexes[i];

    LOGGER_LOG_INFO("{}: Bulkload test finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchSelPackageSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>packageSize, size_t startingEntriesInIndex, size_t entriesToInsert, size_t rsearches, double sel, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    if (labels.size() != packageSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != packageSize.size() {}", exName, labels.size(), packageSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string bulkloadPackageSizeString =  std::string("{") + std::accumulate(std::begin(packageSize), std::end(packageSize), std::string(), buildStringFromVector) + std::string("}");


    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = ") + bulkloadPackageSizeString +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST started: {}", exName, label);

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};

    std::vector<Workload> workloads;
    for (size_t j = 0; j < packageSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST {} / {} started", exName, j + 1, packageSize.size());

        Disk* disk = new DiskSSD_Samsung840();

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
        DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST {} / {} Topology created with {} entries", exName, j + 1, packageSize.size(), startingEntriesInIndex);
        }

        std::vector<WorkloadStep*> steps;

        size_t entriesLeft = entriesToInsert;
        while (entriesLeft > 0)
        {
            size_t n = std::min(entriesLeft, packageSize[j]);
            steps.push_back(new WorkloadStepBulkload(n));

            entriesLeft -= n;

            steps.push_back(new WorkloadStepRSearch(sel, rsearches));
        }

        Workload workload(indexes, steps);
        workload.run();

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload packageSizeTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        for (size_t i = 0; i < indexes.size(); ++i)
            delete indexes[i];
    }

    std::string toPrint("Testing bulkloadSizePackage\t");
    toPrint += label + std::string("\n\n");

    if (rsearches > 0)
    {
        toPrint += std::string("BulkloadTime\n\n");
        toPrint += std::string("BulkloadPackageSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + (names[j] == std::string("LSMTree insert") ? std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\nRSearchTime\n\n");
        toPrint += std::string("BulkloadPackageSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\n");
    }

    toPrint += std::string("TotalTime\n\n");
    toPrint += std::string("BulkloadPackageSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }

    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;


    LOGGER_LOG_INFO("{}: Bulkload packageSizeTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchSelSSTableSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>ssTableSize, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t bufferSize, size_t levelRatio)
{
    if (labels.size() != ssTableSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != ssTableSize.size() {}", exName, labels.size(), ssTableSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string SSTableSizeString =  std::string("{") + std::accumulate(std::begin(ssTableSize), std::end(ssTableSize), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + SSTableSizeString +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload ssTableTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < ssTableSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload ssTableTEST {} / {} started", exName, j + 1, ssTableSize.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm1 = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 1);
        DBIndex* falsm2 = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 2);
        DBIndex* falsm3 = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 3);
        DBIndex* falsm4 = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 4);
        DBIndex* falsm5 = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 5);
        DBIndex* falsm10 = new FALSMTree(names[5].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 10);
        DBIndex* falsm15 = new FALSMTree(names[6].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 15);
        DBIndex* falsm20 = new FALSMTree(names[7].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 20);
        DBIndex* falsm25 = new FALSMTree(names[8].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 25);
        DBIndex* falsm50 = new FALSMTree(names[9].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, 50);
        DBIndex* lsmClassic = new LSMTree(names[10].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize[j], bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload ssTableTEST {} / {} Topology created with {} entries", exName, j + 1, ssTableSize.size(), startingEntriesInIndex);
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

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload ssTableTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        for (size_t i = 0; i < indexes.size(); ++i)
            delete indexes[i];
    }

    std::string toPrint("Testing SSTable\t");
    toPrint += label + std::string("\n\n");

    if (rsearches > 0)
    {
        toPrint += std::string("BulkloadTime\n\n");
        toPrint += std::string("SSTableSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + (names[j] == std::string("LSMTree insert") ? std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\nRSearchTime\n\n");
        toPrint += std::string("SSTableSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\n");
    }

    toPrint += std::string("TotalTime\n\n");
    toPrint += std::string("SSTableSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }

    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload ssTableTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchSelBufferSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>bufferSize, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t ssTableSize, size_t levelRatio)
{
    if (labels.size() != bufferSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != bufferSize.size() {}", exName, labels.size(), bufferSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string bufferSizeString =  std::string("{") + std::accumulate(std::begin(bufferSize), std::end(bufferSize), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + bufferSizeString +
                        std::string(" ssTableSize = ") +  std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < bufferSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST {} / {} started", exName, j + 1, bufferSize.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm1 = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 1);
        DBIndex* falsm2 = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 2);
        DBIndex* falsm3 = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 3);
        DBIndex* falsm4 = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 4);
        DBIndex* falsm5 = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 5);
        DBIndex* falsm10 = new FALSMTree(names[5].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 10);
        DBIndex* falsm15 = new FALSMTree(names[6].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 15);
        DBIndex* falsm20 = new FALSMTree(names[7].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 20);
        DBIndex* falsm25 = new FALSMTree(names[8].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 25);
        DBIndex* falsm50 = new FALSMTree(names[9].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, 50);
        DBIndex* lsmClassic = new LSMTree(names[10].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize[j], levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST {} / {} Topology created with {} entries", exName, j + 1, bufferSize.size(), startingEntriesInIndex);
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

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload BufferSizeTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        for (size_t i = 0; i < indexes.size(); ++i)
            delete indexes[i];
    }

    std::string toPrint("Testing bufferSize\t");

    toPrint += label + std::string("\n\n");

    if (rsearches > 0)
    {
        toPrint += std::string("BulkloadTime\n\n");
        toPrint += std::string("BufferSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + (names[j] == std::string("LSMTree insert") ? std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\nRSearchTime\n\n");
        toPrint += std::string("BufferSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\n");
    }

    toPrint += std::string("TotalTime\n\n");
    toPrint += std::string("BufferSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload BufferSizeTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchSelLevelRatio(std::string exName, std::vector<std::string> labels, std::vector<size_t>levelRatio, size_t startingEntriesInIndex, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t ssTableSize, size_t bufferSize)
{
    if (labels.size() != levelRatio.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != levelRatio.size() {}", exName, labels.size(), levelRatio.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string levelRatioString =  std::string("{") + std::accumulate(std::begin(levelRatio), std::end(levelRatio), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + std::to_string(startingEntriesInIndex) +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") +  std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") +  std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + levelRatioString + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < levelRatio.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST {} / {} started", exName, j + 1, levelRatio.size());

        Disk* disk = new DiskSSD_Samsung840();

        DBIndex* falsm1 = new FALSMTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 1);
        DBIndex* falsm2 = new FALSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 2);
        DBIndex* falsm3 = new FALSMTree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 3);
        DBIndex* falsm4 = new FALSMTree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 4);
        DBIndex* falsm5 = new FALSMTree(names[4].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 5);
        DBIndex* falsm10 = new FALSMTree(names[5].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 10);
        DBIndex* falsm15 = new FALSMTree(names[6].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 15);
        DBIndex* falsm20 = new FALSMTree(names[7].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 20);
        DBIndex* falsm25 = new FALSMTree(names[8].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 25);
        DBIndex* falsm50 = new FALSMTree(names[9].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], 50);
        DBIndex* lsmClassic = new LSMTree(names[10].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio[j], LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
        if (startingEntriesInIndex > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(startingEntriesInIndex);

            LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST {} / {} Topology created with {} entries", exName, j + 1, levelRatio.size(), startingEntriesInIndex);
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

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + startingEntriesInIndex)
                LOGGER_LOG_ERROR("{}: Bulkload leveRatioTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        for (size_t i = 0; i < indexes.size(); ++i)
            delete indexes[i];
    }

    std::string toPrint("Testing levelRatio\t");
    toPrint += label + std::string("\n\n");

    if (rsearches > 0)
    {
        toPrint += std::string("BulkloadTime\n\n");
        toPrint += std::string("LevelRatio");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + (names[j] == std::string("LSMTree insert") ? std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\nRSearchTime\n\n");
        toPrint += std::string("LevelRatio");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME));
            toPrint += std::string("\n");
        }
        toPrint += std::string("\n");
    }

    toPrint += std::string("TotalTime\n\n");
    toPrint += std::string("LevelRatio");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload leveRatioTEST finished: {}", exName, label);
}

[[maybe_unused]] static void sandbox_bulkloadAndRSearchSelDatabaseSize(std::string exName, std::vector<std::string> labels, std::vector<size_t>databaseSize, size_t entriesToInsert,  size_t minRandom, size_t maxRandom, size_t rsearches, double sel, size_t ssTableSize, size_t bufferSize, size_t levelRatio)
{
    if (labels.size() != databaseSize.size())
    {
        LOGGER_LOG_ERROR("{} labels.size() {} != databaseSize.size() {}", exName, labels.size(), databaseSize.size());
        return;
    }

    DBTable* table = new DBTable_TPCC_Warehouse();

    auto buildStringFromVector = [](const std::string &accumulator, const size_t& size)
    {
        return accumulator.empty() ? std::to_string(size) : accumulator + ", " + std::to_string(size);
    };

    const std::string databaseSizeString =  std::string("{") + std::accumulate(std::begin(databaseSize), std::end(databaseSize), std::string(), buildStringFromVector) + std::string("}");

    std::string label = std::string("Bulkload sandbox ") + exName + std::string(" :") +
                        std::string(" startingEntriesInIndex = ") + databaseSizeString +
                        std::string(" entriesToInsert = ") + std::to_string(entriesToInsert) +
                        std::string(" bulkloadPackageNumEntries = [") + std::to_string(minRandom) + std::string(" - ") + std::to_string(maxRandom) + std::string("]") +
                        std::string(" rsearches = ") + std::to_string(rsearches) +
                        std::string(" sel = ") + std::to_string(sel) +
                        std::string(" indexesParams = {") +
                        std::string(" bufferSize = ") + std::to_string(bufferSize) +
                        std::string(" ssTableSize = ") + std::to_string(ssTableSize) +
                        std::string(" levelRatio = ") + std::to_string(levelRatio) + std::string(" }") +
                        std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("{}: Bulkload databaseSizeTEST started: {}", exName, label);

    std::vector<Workload> workloads;

    std::vector<std::string> names {"FALSM capRatio=1", "FALSM capRatio=2", "FALSM capRatio=3", "FALSM capRatio=4", "FALSM capRatio=5", "FALSM capRatio=10", "FALSM capRatio=15", "FALSM capRatio=20", "FALSM capRatio=25", "FALSM capRatio=50", "LSMTree insert", "LSMTree bulkload"};
    for (size_t j = 0; j < databaseSize.size(); ++j)
    {
        LOGGER_LOG_INFO("{}: Bulkload databaseSizeTEST {} / {} started", exName, j + 1, databaseSize.size());

        Disk* disk = new DiskSSD_Samsung840();

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
        DBIndex* lsmMaxCap = new LSMTree(names[11].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), ssTableSize, bufferSize, levelRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);

        std::vector<DBIndex*> indexes {falsm1, falsm2, falsm3, falsm4, falsm5, falsm10, falsm15, falsm20, falsm25, falsm50, lsmClassic, lsmMaxCap};
        if (databaseSize[j] > 0)
        {
            for (size_t i = 0; i < indexes.size(); ++i)
                indexes[i]->createTopologyAfterInsert(databaseSize[j]);

            LOGGER_LOG_INFO("{}: Bulkload databaseSizeTEST {} / {} Topology created with {} entries", exName, j + 1, databaseSize.size(), databaseSize[j]);
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

        for (size_t i = 0; i < indexes.size(); ++i)
            if (indexes[i]->getNumEntries() != entriesToInsert + databaseSize[j])
                LOGGER_LOG_ERROR("{}: Bulkload databaseSizeTEST Index {}, numEntries = {} != entriesToInsert {}", exName, indexes[i]->getName(), indexes[i]->getNumEntries(), entriesToInsert);


        workloads.push_back(workload);

        delete disk;
        for (size_t i = 0; i < indexes.size(); ++i)
            delete indexes[i];
    }

    std::string toPrint("Testing databaseSize\t");
    toPrint += label + std::string("\n\n");

    if (rsearches > 0)
    {
        toPrint += std::string("BulkloadTime\n\n");
        toPrint += std::string("databaseSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + (names[j] == std::string("LSMTree insert") ? std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)) : std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\nRSearchTime\n\n");
        toPrint += std::string("databaseSize");
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += std::string("\t") + names[i];
        toPrint += std::string("\n");

        for (size_t i = 0; i < labels.size(); ++i)
        {
            toPrint += labels[i];
            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME));
            toPrint += std::string("\n");
        }

        toPrint += std::string("\n");
    }

    toPrint += std::string("TotalTime\n\n");
    toPrint += std::string("databaseSize");
    for (size_t i = 0; i < names.size(); ++i)
        toPrint += std::string("\t") + names[i];
    toPrint += std::string("\n");

    for (size_t i = 0; i < labels.size(); ++i)
    {
        toPrint += labels[i];
        const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::string("\t") + std::to_string(totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
        toPrint += std::string("\n");
    }
    toPrint += std::string("\n");

    DBThreadPool::mutex.lock();

    std::ofstream outfile;
    std::string path(LSM_SANDBOX_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path, std::ios_base::app);

    outfile << toPrint << std::flush;
    std::cout << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete table;

    LOGGER_LOG_INFO("{}: Bulkload databaseSizeTEST finished: {}", exName, label);
}

/**
 * @brief Main Paper Sandbox function
 *        By calling this function you will execute all
 *        experiments that will help us with uderstanding of algo
 *        and develop paper. Should be used only by internal developers
 *
 */
void sandboxLSMPaper()
{
    LOGGER_LOG_INFO("Starting LSM Sandbox");

    std::filesystem::create_directories(LSM_SANDBOX_DIRECTORY_PATH);

    std::vector<std::future<bool>> futures;

    // simple functions to observe something
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 1000000 / 2, 1000000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 10000 / 2, 10000, 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 10));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 15));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 20));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 22, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 23, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 24, 5));

    // Testing printing etc... on small numbers
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadPackageSize, "exSimple1", std::vector<std::string>{"10k", "50k", "100k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 500000}, 0, 100000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadSSTableSize, "exSimple1", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 100000, 10000 / 2, 10000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadBufferSize, "exSimple1", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 100000, 10000 / 2, 10000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "exSimple1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 100000, 10000 / 2, 10000, 1 << 21, 1 << 21));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadPackageSize, "exSimple2", std::vector<std::string>{"10k", "50k", "100k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 500000}, 100000000LU, 100000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadSSTableSize, "exSimple2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 100000000LU, 100000, 10000 / 2, 10000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadBufferSize, "exSimple2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 100000000LU, 100000, 10000 / 2, 10000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "exSimple2", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 100000000LU, 100000, 10000 / 2, 10000, 1 << 21, 1 << 21));


    // Testing some algo behaviour on bulkoad and search
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5)); // opt 25
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5)); // opt 5
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5)); // opt 2-4
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5)); // opt 1


    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 50, 0.01, 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 5, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 50, 0.01, 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 10, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 0, 10000000, 100000 / 2, 100000, 1000, 0.01, 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 10, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 1000, 0.01, 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 10, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 100, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 0, 10000000, 100000 / 2, 100000, 1000, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 10, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 100, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandom, "exTest", 100000000LU, 10000000, 100000 / 2, 100000, 1000, static_cast<size_t>(10000), 1 << 21, 1 << 21, 5));

    // Real experiments Only bulkload
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadPackageSize, "ex1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadSSTableSize, "ex1", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 1000000, 100000 / 2, 100000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadBufferSize, "ex1", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "ex1", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "ex1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21));

    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadPackageSize, "ex2", std::vector<std::string>{"10k", "50k", "100k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 500000}, 100000000LU, 10000000, 1 << 21, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadSSTableSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 100000000LU, 10000000, 100000 / 2, 100000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadBufferSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 100000000LU, 10000000, 100000 / 2, 100000, 1 << 21, 5));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "ex2", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 100000000LU, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21));
    //futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadLevelRatio, "ex2", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 100000000LU, 10000000, 100000 / 2, 100000, 1 << 21, 1 << 21));


    // Real experiments bulkload + search
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "ex0", 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5)); // opt 25
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "ex0", 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5)); // opt 5
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "ex0", 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5)); // opt 2-4
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchRandomSel, "ex0", 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5)); // opt 1

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelPackageSize, "ex1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 0, 0.0, 1 << 21, 1 << 21, 5));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelPackageSize, "ex1.1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 20, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelPackageSize, "ex1.1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 40, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelPackageSize, "ex1.1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 100, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelPackageSize, "ex1.1", std::vector<std::string>{"10k", "50k", "100k", "200k", "300k", "400k", "500k"}, std::vector<size_t>{10000, 50000, 100000, 200000, 300000, 400000, 500000}, 0, 10000000, 250, 0.01, 1 << 21, 1 << 21, 5));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelSSTableSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelSSTableSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelSSTableSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelSSTableSize, "ex2", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 5));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelBufferSize, "ex3", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelBufferSize, "ex3", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelBufferSize, "ex3", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelBufferSize, "ex3", std::vector<std::string>{"2MB", "4MB", "8MB", "16MB"}, std::vector<size_t>{1 << 21, 1 << 22, 1 << 23, 1 << 24}, 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 5));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4", std::vector<std::string>{"5", "10", "15", "20", "25"}, std::vector<size_t>{5, 10, 15, 20, 25}, 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4.1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4.1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4.1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelLevelRatio, "ex4.1", std::vector<std::string>{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}, std::vector<size_t>{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21));

    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelDatabaseSize, "ex5", std::vector<std::string>{"0mln", "10mln", "20mln", "30mln", "40mln", "50mln", "60mln", "70mln", "80mln", "90mln", "100mln"}, std::vector<size_t>{0, 10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000}, 10000000, 100000 / 2, 100000, 20, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelDatabaseSize, "ex5", std::vector<std::string>{"0mln", "10mln", "20mln", "30mln", "40mln", "50mln", "60mln", "70mln", "80mln", "90mln", "100mln"}, std::vector<size_t>{0, 10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000}, 10000000, 100000 / 2, 100000, 40, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelDatabaseSize, "ex5", std::vector<std::string>{"0mln", "10mln", "20mln", "30mln", "40mln", "50mln", "60mln", "70mln", "80mln", "90mln", "100mln"}, std::vector<size_t>{0, 10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000}, 10000000, 100000 / 2, 100000, 100, 0.01, 1 << 21, 1 << 21, 5));
    futures.push_back(DBThreadPool::threadPool.submit(sandbox_bulkloadAndRSearchSelDatabaseSize, "ex5", std::vector<std::string>{"0mln", "10mln", "20mln", "30mln", "40mln", "50mln", "60mln", "70mln", "80mln", "90mln", "100mln"}, std::vector<size_t>{0, 10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000}, 10000000, 100000 / 2, 100000, 250, 0.01, 1 << 21, 1 << 21, 5));

    for (size_t i = 0; i < futures.size(); ++i)
    {
        bool isComplete = futures[i].get();
        (void) isComplete;
    }

    LOGGER_LOG_INFO("Finished {} tasks from LSM Sandbox", futures.size());
}


/**
 * @brief Main Paper experiment functions.
 *        By calling this function you will execute exeriments from final paper
 *
 */
void experimentLSMPaper()
{
    LOGGER_LOG_INFO("Starting LSM paper experiments");


    LOGGER_LOG_INFO("LSM paper experiments finished");
}
