#include <logger/logger.hpp>
#include <table/dbTable.hpp>
#include <table/dbTableTPCC.hpp>
#include <disk/diskSSD.hpp>
#include <workload/workloadLib.hpp>
#include <index/fdtree.hpp>
#include <index/fbdsm.hpp>
#include <index/dbIndexRawToColumnWrapper.hpp>
#include <adaptiveMerging/adaptiveMerging.hpp>
#include <adaptiveMerging/lazyAdaptiveMerging.hpp>
#include <threadPool/dbThreadPool.hpp>

#include <random>
#include <iostream>
#include <numeric>
#include <fstream>
#include <filesystem>

#define LAM_SANDBOX_DIRECTORY_PATH          "../experimentResults/phd/lam/sandbox"
#define LAM_REAL_EXPERIMENTS_DIRECTORY_PATH "../experimentResults/phd/lam/real"

#define MY_LUCKY_SEED 235111741 // euler lucky numbers 2, 3, 5, 11, 7, 41

[[maybe_unused]] static void ex1_phd_basic_step(const std::string& exName, Disk* disk, const DBTable* table, size_t startingEntries,const double sel)
{
    std::vector<std::string> relativeIndexesNames {"FD-Tree", "Skan"};
    DBIndex* fdtree_raw = new FDTree(relativeIndexesNames[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
    DBIndexColumn* fdtree = new DBIndexRawToColumnWrapper(fdtree_raw, table->getAllColumnSize());

    DBIndexColumn* fbdsm = new FBDSM(relativeIndexesNames[1].c_str(), disk->clone(), table->getAllColumnSize());
    std::vector<DBIndexColumn*> relativeIndexes {fdtree, fbdsm};

    for (size_t j = 0; j < relativeIndexes.size(); ++j)
        relativeIndexes[j]->createTopologyAfterInsert(startingEntries);

    std::vector<size_t> cols;
    for (size_t j = 0; j < table->getNumColumns(); ++j)
        cols.push_back(j);

    std::vector<WorkloadStep*> steps;
    steps.push_back(new WorkloadStepRSearch(sel, cols, 1));

    Workload fixedWorkload(relativeIndexes, steps);
    fixedWorkload.run();

    const std::vector<WorkloadCounters>& fixedWorkloadtotalCounters = fixedWorkload.getAllTotalCounters();
    const double fdTime = fixedWorkloadtotalCounters[0].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME);
    const double scanTime = fixedWorkloadtotalCounters[1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME);

    std::vector<std::string> names {"Adaptive Merging", "Lazy Adaptive Merging"};

    DBIndex* fdtree_am = new FDTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
    DBIndex* fdtree_lam = new FDTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);

    const size_t paritionSize = (1 << 20) * 10;
    DBIndex* am = new AdaptiveMerging(names[0].c_str(), fdtree_am, startingEntries, paritionSize);

    const size_t reorganizationMaxBlocks = 40000;
    const size_t reorganizationBlocksTreshold = 4000;
    const double lebUsageTreshold = 0.25;
    DBIndex* lam = new LazyAdaptiveMerging(names[1].c_str(), fdtree_lam, startingEntries, paritionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    std::vector<DBIndex*> indexes {am, lam};

    Workload* w = new WorkloadAdaptiveMerging(indexes, sel);

    LOGGER_LOG_INFO("PHD {} started", exName);

    w->run();

    LOGGER_LOG_INFO("PHD {} Workload finished", exName);

    // TIME per query
    {
        const std::vector<std::vector<WorkloadCounters>>& queryCounters = w->getAllStepCounters();
        const size_t queries = std::min(queryCounters[0].size(), queryCounters[1].size());

        std::string toPrint = "Query\t";

        for (size_t i = 0; i < relativeIndexesNames.size(); ++i)
            toPrint += relativeIndexesNames[i] + std::string("\t");

        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));


        for (size_t q = 0; q < queries; ++q)
        {
            toPrint += std::to_string(q + 1) + "\t" +  std::to_string(fdTime) + "\t" + std::to_string(scanTime) + "\t";
            for (size_t idx = 0; idx < queryCounters.size(); ++idx)
            {
                toPrint += std::to_string(queryCounters[idx][q].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) +
                (idx != queryCounters.size() - 1 ? std::string("\t") : std::string("\n"));
            }
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_query_time") +std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // REAL time
    {
        std::string toPrint = "";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        const std::vector<WorkloadCounters>& totalCounters = w->getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    // REAL memory wearout
    {
        std::string toPrint = "";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        const std::vector<WorkloadCounters>& totalCounters = w->getAllTotalCounters();
        for (size_t j = 0; j < totalCounters.size(); ++j)
            toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
                    (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    delete disk;
    delete table;
    delete w;

    LOGGER_LOG_INFO("PHD {} finished", exName);
}


[[maybe_unused]] static void ex2_phd_extendend_step(const std::string& exName, Disk* disk, const DBTable* table, const double sel)
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
                                               sel,
                                               5);

    WorkloadExperiment wb = WorkloadExperiment(std::string("ZR_{B}"),
                                               5,
                                               100000,
                                               5,
                                               sel,
                                               100000);

    WorkloadExperiment wc = WorkloadExperiment(std::string("ZR_{C}"),
                                               10,
                                               10000000,
                                               20,
                                               sel,
                                               1000000);

    WorkloadExperiment wd = WorkloadExperiment(std::string("ZR_{D}"),
                                               10,
                                               1000000,
                                               10,
                                               sel,
                                               10000);

    // WorkloadExperiment wtest = WorkloadExperiment(std::string("ZR_{TEST}"),
    //                                               100,
    //                                               0,
    //                                               1,
    //                                               sel,
    //                                               0);


    std::vector<WorkloadExperiment> wexperiments {wa, wb, /*wc, */ wd};
    // std::vector<WorkloadExperiment> wexperiments {wtest};
    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"Adaptive Merging", "Lazy Adaptive Merging"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* fdtree_am = new FDTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
        DBIndex* fdtree_lam = new FDTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);

        const size_t paritionSize = (1 << 20) * 10;
        DBIndex* am = new AdaptiveMerging(names[0].c_str(), fdtree_am, startingEntries, paritionSize);
        const size_t reorganizationMaxBlocks = 40000;
        const size_t reorganizationBlocksTreshold = 4000;
        const double lebUsageTreshold = 0.25;
        DBIndex* lam = new LazyAdaptiveMerging(names[1].c_str(), fdtree_lam, startingEntries, paritionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

        std::vector<DBIndex*> indexes {am, lam};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));
            steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, wexperiments[i].qrsearch));
            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
        }
        Workload workload(indexes, steps);

        // for (size_t j = 0; j < indexes.size(); ++j)
        //     indexes[j]->createTopologyAfterInsert(startingEntries);

        // LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

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
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to LAM time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double lamTime = std::max(1.0, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) / lamTime) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
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
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to LAM wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double lamWearout = std::max(1L, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / lamWearout) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);
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
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_warehouse_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_warehouse_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_warehouse_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_warehouse_5", disk, table, 10000000, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_warehouse_5", disk, table, 10000000, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_warehouse_5", disk, table, 10000000, 0.05));
    }



    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_customer_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_customer_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_customer_1", disk, table, 10000000, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_customer_5", disk, table, 10000000, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_customer_5", disk, table, 10000000, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_customer_5", disk, table, 10000000, 0.05));
    }
}

[[maybe_unused]] static void ex_phd_extendend_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_samsung_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_toshiba_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_intel_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_samsung_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_toshiba_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_intel_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_samsung_tpcc_customer_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_toshiba_tpcc_customer_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_extendend_step, "ex2_extendend_intel_tpcc_customer_1", disk, table, 0.01));
    }
}


/**
 *
 * @brief Main PhD experiment functions for Lazy Adaptive Merging
 *        By calling this function you will execute exeriments from PhD Thesis
 *
 */
void experimentPhdLAM()
{
    LOGGER_LOG_INFO("Starting LazyAdaptiveMerging PHD experiments");

    std::filesystem::create_directories(LAM_REAL_EXPERIMENTS_DIRECTORY_PATH);

    std::vector<std::future<bool>> futures;

    ex_phd_basic_batch(futures);
    ex_phd_extendend_batch(futures);

    LOGGER_LOG_INFO("LAM PHD experiments {} job created", futures.size());
    for (size_t i = 0; i < futures.size(); ++i)
    {
        bool isComplete = futures[i].get();
        (void) isComplete;
    }

    LOGGER_LOG_INFO("Finished {} tasks from LAM PHD experiments", futures.size());
}

