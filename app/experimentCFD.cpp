#include <logger/logger.hpp>
#include <table/dbTable.hpp>
#include <table/dbTableTPCC.hpp>
#include <workload/workloadLib.hpp>
#include <threadPool/dbThreadPool.hpp>

#include <disk/diskSSD.hpp>
#include <index/dbIndexRawToColumnWrapper.hpp>
#include <index/fbdsm.hpp>
#include <index/cfdtree.hpp>
#include <index/fdtree.hpp>
#include <index/dbIndexColumn.hpp>

#include <random>
#include <iostream>
#include <numeric>
#include <fstream>
#include <filesystem>

#define CFD_SANDBOX_DIRECTORY_PATH          "../experimentResults/phd/cfd/sandbox"
#define CFD_REAL_EXPERIMENTS_DIRECTORY_PATH "../experimentResults/phd/cfd/real"

#define MY_LUCKY_SEED 235111741 // euler lucky numbers 2, 3, 5, 11, 7, 41

[[maybe_unused]] static void ex1_phd_basic_step(const std::string& exName, size_t quries, Disk* disk, const DBTable* table)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t qinsert;
        size_t qpsearch;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t qinsert, size_t qpsearch, size_t qdelete)
        : name{name}, qinsert{qinsert}, qpsearch{qpsearch}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qpsearch: " + std::to_string(qpsearch) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };

    WorkloadExperiment insertIntensive = WorkloadExperiment(std::string("ZP_{zapis}"),
                                                            static_cast<size_t>(quries * 0.6),
                                                            static_cast<size_t>(quries * 0.2),
                                                            static_cast<size_t>(quries * 0.2));

    WorkloadExperiment searchIntensive = WorkloadExperiment(std::string("ZP_{odczyt}"),
                                                            static_cast<size_t>(quries * 0.15),
                                                            static_cast<size_t>(quries * 0.8),
                                                            static_cast<size_t>(quries * 0.05));

    WorkloadExperiment balanced = WorkloadExperiment(std::string("ZP_{balans}"),
                                                     static_cast<size_t>(quries * 0.375),
                                                     static_cast<size_t>(quries * 0.5),
                                                     static_cast<size_t>(quries * 0.125));

    std::vector<WorkloadExperiment> wexperiments {insertIntensive, searchIntensive, balanced};

    // std::string label = std::string("PHD Fa basic ") + exName + std::string(" :") +
    //                     std::string(" quries = ") + std::to_string(quries) +
    //                     std::string(" workloads:[{") + insertIntensive.toString() + "}, {" + searchIntensive.toString()  + "}, {" + balanced.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<std::vector<Workload>> workloads;
    std::vector<std::string> names {"FD-Tree", "FBDSM", "CFD"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::vector<Workload> colWorkload;

        for (size_t col = 1; col < table->getNumColumns(); ++col)
        {
            DBIndex* fdtree_raw = new FDTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
            DBIndexColumn* fdtree = new DBIndexRawToColumnWrapper(fdtree_raw, table->getAllColumnSize());

            DBIndexColumn* fbdsm = new FBDSM(names[1].c_str(), disk->clone(), table->getAllColumnSize());
            DBIndexColumn* cfd = new CFDTree(names[2].c_str(), disk->clone(), table->getAllColumnSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
            std::vector<DBIndexColumn*> indexes {fdtree, fbdsm, cfd};

            std::vector<WorkloadStep*> steps;
            steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));

            std::vector<size_t> cols;
            for (size_t j = 0; j <= col; ++j)
                cols.push_back(j);

            steps.push_back(new WorkloadStepPSearch(wexperiments[i].qpsearch, cols));

            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));

            Workload workload(indexes, steps);
            workload.run();

            colWorkload.push_back(workload);

            for (size_t j = 0; j < indexes.size(); ++j)
                delete indexes[j];
        }

        workloads.push_back(colWorkload);
    }

    // REAL time
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::string toPrint = "Liczba zwracanych kolumn\t";
        for (size_t j = 0; j < names.size(); ++j)
            toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t j = 0; j < workloads[i].size(); ++j)
        {
            toPrint += std::to_string(j + 2) + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
            for (size_t k = 0; k < totalCounters.size(); ++k)
                toPrint += std::to_string(std::max(1.0, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    // // REAL memory wearout
    // for (size_t i = 0; i < wexperiments.size(); ++i)
    // {
    //     std::string toPrint = "Liczba zwracanych kolumn\t";
    //     for (size_t j = 0; j < names.size(); ++j)
    //         toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

    //     for (size_t j = 0; j < workloads[i].size(); ++j)
    //     {
    //         toPrint += std::to_string(j + 2) + std::string("\t");

    //         const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
    //         for (size_t k = 0; k < totalCounters.size(); ++k)
    //             toPrint += std::to_string(std::max(1L, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
    //                     (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
    //     }

    //     LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
    //     DBThreadPool::mutex.lock();

    //     std::ofstream outfile;
    //     std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
    //     path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string("_wearout") + std::string(".txt");
    //     outfile.open(path);

    //     outfile << toPrint << std::flush;

    //     DBThreadPool::mutex.unlock();
    // }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex2_phd_basic_step(const std::string& exName, size_t quries, Disk* disk, const DBTable* table, double sel)
{
    struct WorkloadExperiment
    {
        std::string name;
        size_t qinsert;
        size_t qrsearch;
        size_t qdelete;

        WorkloadExperiment(const std::string& name, size_t qinsert, size_t qrsearch, size_t qdelete)
        : name{name}, qinsert{qinsert}, qrsearch{qrsearch}, qdelete{qdelete}
        {

        }

        std::string toString() const noexcept(true)
        {
            return "name: " + name +
                   ", qinsert: " + std::to_string(qinsert) +
                   ", qrsearch: " + std::to_string(qrsearch) +
                   ", qdelete: " + std::to_string(qdelete);
        }
    };

    WorkloadExperiment insertIntensive = WorkloadExperiment(std::string("ZP_{zapis}"),
                                                            static_cast<size_t>(quries * 0.6),
                                                            static_cast<size_t>(quries * 0.2),
                                                            static_cast<size_t>(quries * 0.2));

    WorkloadExperiment searchIntensive = WorkloadExperiment(std::string("ZP_{odczyt}"),
                                                            static_cast<size_t>(quries * 0.15),
                                                            static_cast<size_t>(quries * 0.8),
                                                            static_cast<size_t>(quries * 0.05));

    WorkloadExperiment balanced = WorkloadExperiment(std::string("ZP_{balans}"),
                                                     static_cast<size_t>(quries * 0.375),
                                                     static_cast<size_t>(quries * 0.5),
                                                     static_cast<size_t>(quries * 0.125));

    std::vector<WorkloadExperiment> wexperiments {insertIntensive, searchIntensive, balanced};

    // std::string label = std::string("PHD Fa basic ") + exName + std::string(" :") +
    //                     std::string(" quries = ") + std::to_string(quries) +
    //                     std::string(" workloads:[{") + insertIntensive.toString() + "}, {" + searchIntensive.toString()  + "}, {" + balanced.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<std::vector<Workload>> workloads;
    std::vector<std::string> names {"FD-Tree", "FBDSM", "CFD"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::vector<Workload> colWorkload;

        for (size_t col = 1; col < table->getNumColumns(); ++col)
        {
            DBIndex* fdtree_raw = new FDTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
            DBIndexColumn* fdtree = new DBIndexRawToColumnWrapper(fdtree_raw, table->getAllColumnSize());

            DBIndexColumn* fbdsm = new FBDSM(names[1].c_str(), disk->clone(), table->getAllColumnSize());
            DBIndexColumn* cfd = new CFDTree(names[2].c_str(), disk->clone(), table->getAllColumnSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
            std::vector<DBIndexColumn*> indexes {fdtree, fbdsm, cfd};

            std::vector<WorkloadStep*> steps;
            steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));

            std::vector<size_t> cols;
            for (size_t j = 0; j <= col; ++j)
                cols.push_back(j);

            steps.push_back(new WorkloadStepRSearch(sel, cols, wexperiments[i].qrsearch));

            steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));

            Workload workload(indexes, steps);
            workload.run();

            colWorkload.push_back(workload);

            for (size_t j = 0; j < indexes.size(); ++j)
                delete indexes[j];
        }

        workloads.push_back(colWorkload);
    }

    // REAL time
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::string toPrint = "Liczba zwracanych kolumn\t";
        for (size_t j = 0; j < names.size(); ++j)
            toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t j = 0; j < workloads[i].size(); ++j)
        {
            toPrint += std::to_string(j + 2) + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
            for (size_t k = 0; k < totalCounters.size(); ++k)
                toPrint += std::to_string(std::max(1.0, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    // // REAL memory wearout
    // for (size_t i = 0; i < wexperiments.size(); ++i)
    // {
    //     std::string toPrint = "Liczba zwracanych kolumn\t";
    //     for (size_t j = 0; j < names.size(); ++j)
    //         toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

    //     for (size_t j = 0; j < workloads[i].size(); ++j)
    //     {
    //         toPrint += std::to_string(j + 2) + std::string("\t");

    //         const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
    //         for (size_t k = 0; k < totalCounters.size(); ++k)
    //             toPrint += std::to_string(std::max(1L, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
    //                     (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
    //     }

    //     LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
    //     DBThreadPool::mutex.lock();

    //     std::ofstream outfile;
    //     std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
    //     path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string("_wearout") + std::string(".txt");
    //     outfile.open(path);

    //     outfile << toPrint << std::flush;

    //     DBThreadPool::mutex.unlock();
    // }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}


[[maybe_unused]] static void ex3_phd_extendend_step(const std::string& exName, Disk* disk, const DBTable* table, double sel)
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



    std::vector<WorkloadExperiment> wexperiments {wa, wb, /*wc,*/ wd};

    // std::string label = std::string("PHD Fa basic ") + exName + std::string(" :") +
    //                     std::string(" quries = ") + std::to_string(quries) +
    //                     std::string(" workloads:[{") + insertIntensive.toString() + "}, {" + searchIntensive.toString()  + "}, {" + balanced.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<std::vector<Workload>> workloads;
    std::vector<std::string> names {"FD-Tree", /* "FBDSM" ,*/ "CFD"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::vector<Workload> colWorkload;

        for (size_t col = 1; col < table->getNumColumns(); ++col)
        {
            LOGGER_LOG_INFO("PHD {}: workload {}, cols {} started\n", exName, wexperiments[i].name, col);
            const size_t bufferSize = (disk->getLowLevelController().getBlockSize() / table->getKeySize()) * table->getRecordSize();
            DBIndex* fdtree_raw = new FDTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), bufferSize, bufferSize, 5);
            DBIndexColumn* fdtree = new DBIndexRawToColumnWrapper(fdtree_raw, table->getAllColumnSize());

            // DBIndexColumn* fbdsm = new FBDSM(names[1].c_str(), disk->clone(), table->getAllColumnSize());
            DBIndexColumn* cfd = new CFDTree(names[1].c_str(), disk->clone(), table->getAllColumnSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
            std::vector<DBIndexColumn*> indexes {fdtree, /*fbdsm,*/ cfd};

            std::vector<WorkloadStep*> steps;

            std::vector<size_t> cols;
            for (size_t j = 0; j <= col; ++j)
                cols.push_back(j);

            for (size_t j = 0; j < wexperiments[i].series; ++j)
            {
                steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));
                steps.push_back(new WorkloadStepRSearch(wexperiments[i].sel, cols, wexperiments[i].qrsearch));
                steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));
            }

            Workload workload(indexes, steps);

            for (size_t j = 0; j < indexes.size(); ++j)
                indexes[j]->createTopologyAfterInsert(startingEntries);

            // LOGGER_LOG_INFO("PHD {} {}: Topology created with {} entries for all indexes", exName,  wexperiments[i].name, startingEntries);

            workload.run();

            colWorkload.push_back(workload);

            for (size_t j = 0; j < indexes.size(); ++j)
                delete indexes[j];
        }

        workloads.push_back(colWorkload);
    }

    // REAL time
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        std::string toPrint = "Liczba zwracanych kolumn\t";
        for (size_t j = 0; j < names.size(); ++j)
            toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t j = 0; j < workloads[i].size(); ++j)
        {
            toPrint += std::to_string(j + 2) + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
            for (size_t k = 0; k < totalCounters.size(); ++k)
                toPrint += std::to_string(std::max(1.0, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME))) +
                        (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT REAL TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }


    // // REAL memory wearout
    // for (size_t i = 0; i < wexperiments.size(); ++i)
    // {
    //     std::string toPrint = "Liczba zwracanych kolumn\t";
    //     for (size_t j = 0; j < names.size(); ++j)
    //         toPrint += names[j] + (j == names.size() - 1 ? std::string("\n") : std::string("\t"));

    //     for (size_t j = 0; j < workloads[i].size(); ++j)
    //     {
    //         toPrint += std::to_string(j + 2) + std::string("\t");

    //         const std::vector<WorkloadCounters>& totalCounters = workloads[i][j].getAllTotalCounters();
    //         for (size_t k = 0; k < totalCounters.size(); ++k)
    //             toPrint += std::to_string(std::max(1L, totalCounters[k].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / 1000000) +
    //                     (k != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
    //     }

    //     LOGGER_LOG_INFO("PHD {}: TO PRINT REAL WEAROUT\n{}", exName, toPrint);
    //     DBThreadPool::mutex.lock();

    //     std::ofstream outfile;
    //     std::string path(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);
    //     path += std::string("/") + exName + std::string("_") + wexperiments[i].name + std::string("_wearout") + std::string(".txt");
    //     outfile.open(path);

    //     outfile << toPrint << std::flush;

    //     DBThreadPool::mutex.unlock();
    // }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex1_phd_basic_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_warehouse", 1000000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_warehouse", 1000000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_warehouse", 1000000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_samsung_tpcc_customer", 1000000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_toshiba_tpcc_customer", 1000000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex1_phd_basic_step, "ex1_basic_intel_tpcc_customer", 1000000, disk, table));
    }
}

[[maybe_unused]] static void ex2_phd_basic_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_warehouse", 1000000, disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_warehouse", 1000000, disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_warehouse", 1000000, disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_customer", 1000000, disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_customer", 1000000, disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_customer", 1000000, disk, table, 0.01));
    }


    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_warehouse_5", 1000000, disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_warehouse_5", 1000000, disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_warehouse_5", 1000000, disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_customer_5", 1000000, disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_customer_5", 1000000, disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_customer_5", 1000000, disk, table, 0.05));
    }



    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_warehouse_10", 1000000, disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_warehouse_10", 1000000, disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_warehouse_10", 1000000, disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_samsung_tpcc_customer_10", 1000000, disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_toshiba_tpcc_customer_10", 1000000, disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex2_phd_basic_step, "ex2_basic_intel_tpcc_customer_10", 1000000, disk, table, 0.1));
    }
}


[[maybe_unused]] static void ex3_phd_extendend_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_customer_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_customer_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_customer_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_customer_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_customer_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_customer_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_customer_10", disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_customer_10", disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_customer_10", disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_warehouse_1", disk, table, 0.01));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_warehouse_5", disk, table, 0.05));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_Samsung840();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_samsung_tpcc_warehouse_10", disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_ToshibaVX500();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_toshiba_tpcc_warehouse_10", disk, table, 0.1));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskSSD_IntelDCP4511();
        futures.push_back(DBThreadPool::threadPool.submit(ex3_phd_extendend_step, "ex3_extendend_intel_tpcc_warehouse_10", disk, table, 0.1));
    }
}

/**
 *
 * @brief Main PhD experiment functions for CFD Tree
 *        By calling this function you will execute exeriments from PhD Thesis
 *
 */
void experimentPhdCFDTree()
{
    LOGGER_LOG_INFO("Starting CFDTree PHD experiments");

    std::filesystem::create_directories(CFD_REAL_EXPERIMENTS_DIRECTORY_PATH);

    std::vector<std::future<bool>> futures;

    // ex1_phd_basic_batch(futures);
    // ex2_phd_basic_batch(futures);
    ex3_phd_extendend_batch(futures);

    LOGGER_LOG_INFO("CFD PHD experiments {} job created", futures.size());
    for (size_t i = 0; i < futures.size(); ++i)
    {
        bool isComplete = futures[i].get();
        (void) isComplete;
    }

    LOGGER_LOG_INFO("Finished {} tasks from CFD PHD experiments", futures.size());
}
