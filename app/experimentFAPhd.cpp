#include <logger/logger.hpp>
#include <table/dbTable.hpp>
#include <table/dbTableTPCC.hpp>
#include <disk/diskFlashNandFTL.hpp>
#include <workload/workloadLib.hpp>
#include <index/bptree.hpp>
#include <index/fatree.hpp>
#include <index/lsmtree.hpp>
#include <threadPool/dbThreadPool.hpp>

#include <random>
#include <iostream>
#include <numeric>
#include <fstream>
#include <filesystem>

#define FA_SANDBOX_DIRECTORY_PATH          "../experimentResults/phd/fa/sandbox"
#define FA_REAL_EXPERIMENTS_DIRECTORY_PATH "../experimentResults/phd/fa/real"

#define MY_LUCKY_SEED 235111741 // euler lucky numbers 2, 3, 5, 11, 7, 41

[[maybe_unused]] static void ex_phd_several_k_step(const std::string& exName, size_t quries, Disk* disk, const DBTable* table)
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

    // std::string label = std::string("PHD Fa several K ") + exName + std::string(" :") +
    //                     std::string(" quries = ") + std::to_string(quries) +
    //                     std::string(" workloads:[{") + insertIntensive.toString() + "}, {" + searchIntensive.toString()  + "}, {" + balanced.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"K=5", "K=10", "K=25", "K=50"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* fatree5 = new FATree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);
        DBIndex* fatree10 = new FATree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 10);
        DBIndex* fatree15 = new FATree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 25);
        DBIndex* fatree20 = new FATree(names[3].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 50);


        std::vector<DBIndex*> indexes {fatree5, fatree10, fatree15, fatree20};

        std::vector<WorkloadStep*> steps;
        steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));
        steps.push_back(new WorkloadStepPSearch(wexperiments[i].qpsearch));
        steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));

        Workload workload(indexes, steps);
        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];
    }

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
    std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
    path += std::string("/") + exName + std::string(".txt");
    outfile.open(path);

    outfile << toPrint << std::flush;

    DBThreadPool::mutex.unlock();

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}

[[maybe_unused]] static void ex_phd_basic_step(const std::string& exName, size_t quries, Disk* disk, const DBTable* table)
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

    std::vector<Workload> workloads;
    std::vector<std::string> names {"B+-Tree", "LSM-Tree", "FA-Tree"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* bptree = new BPTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getPageSize() * 2);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 22, disk->getLowLevelController().getBlockSize(), 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* fatree = new FATree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);

        std::vector<DBIndex*> indexes {bptree, lsmClassic, fatree};

        std::vector<WorkloadStep*> steps;
        steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));
        steps.push_back(new WorkloadStepPSearch(wexperiments[i].qpsearch));
        steps.push_back(new WorkloadStepDelete(wexperiments[i].qdelete));

        Workload workload(indexes, steps);
        workload.run();

        workloads.push_back(workload);

        for (size_t j = 0; j < indexes.size(); ++j)
            delete indexes[j];
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
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to FA-Tree time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double faTime = std::max(1.0, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) / faTime) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
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
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to FA-Tree wearout
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double fawearout = std::max(1L, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1L, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / fawearout) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED WEAROUT\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_wearout_normalized") +  std::string(".txt");
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

    // std::string label = std::string("PHD Fa extendend ") + exName + std::string(" :") +
    //                     std::string(" workloads:[{") + wa.toString() + "}, {" + wb.toString()  + "}, {" + wc.toString()  + "}" + "}, {" + wd.toString() + "}]" +
    //                     std::string(" table = ") + table->toString();

    LOGGER_LOG_INFO("PHD {} started", exName);

    std::vector<Workload> workloads;
    std::vector<std::string> names {"B+-Tree", "LSM-Tree", "FA-Tree"};
    for (size_t i = 0; i < wexperiments.size(); ++i)
    {
        DBIndex* bptree = new BPTree(names[0].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getPageSize() * 2);
        DBIndex* lsmClassic = new LSMTree(names[1].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), 1 << 22, disk->getLowLevelController().getBlockSize(), 5, LSMTree::BULKLOAD_FEATURE_OFF);
        DBIndex* fatree = new FATree(names[2].c_str(), disk->clone(), table->getKeySize(), table->getDataSize(), disk->getLowLevelController().getBlockSize(), disk->getLowLevelController().getBlockSize(), 5);

        std::vector<DBIndex*> indexes {bptree, lsmClassic, fatree};

        std::vector<WorkloadStep*> steps;

        for (size_t j = 0; j < wexperiments[i].series; ++j)
        {
            steps.push_back(new WorkloadStepInsert(wexperiments[i].qinsert));
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
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    // normalized to FA-Tree time
    {
        std::string toPrint = "Workload\t";
        for (size_t i = 0; i < names.size(); ++i)
            toPrint += names[i] + (i == names.size() - 1 ? std::string("\n") : std::string("\t"));

        for (size_t i = 0; i < wexperiments.size(); ++i)
        {
            toPrint += wexperiments[i].name + std::string("\t");

            const std::vector<WorkloadCounters>& totalCounters = workloads[i].getAllTotalCounters();
            const double faTime = std::max(1.0, totalCounters[totalCounters.size() - 1].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));
            for (size_t j = 0; j < totalCounters.size(); ++j)
                toPrint += std::to_string(std::max(1.0, totalCounters[j].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME)) / faTime) +
                        (j != totalCounters.size() -1 ? std::string("\t") : std::string("\n"));
        }

        LOGGER_LOG_INFO("PHD {}: TO PRINT NORMALIZED TIME\n{}", exName, toPrint);
        DBThreadPool::mutex.lock();

        std::ofstream outfile;
        std::string path(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);
        path += std::string("/") + exName + std::string("_normalized") +  std::string(".txt");
        outfile.open(path);

        outfile << toPrint << std::flush;

        DBThreadPool::mutex.unlock();
    }

    delete disk;
    delete table;
    LOGGER_LOG_INFO("PHD {} finished", exName);
}


[[maybe_unused]] static void ex_phd_several_k_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_samsungK9F1G_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_micronMT29AAA_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_micronMT29_WC1_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_samsungK9F1G_tpcc_neworder", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_micronMT29AAA_tpcc_neworder", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_several_k_step, "ex0_several_k_micronMT29_WC1_tpcc_neworder", 100000, disk, table));
    }
}


[[maybe_unused]] static void ex_phd_basic_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_samsungK9F1G_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_micronMT29AAA_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_micronMT29_WC1_tpcc_warehouse", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_samsungK9F1G_tpcc_neworder", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_micronMT29AAA_tpcc_neworder", 100000, disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_basic_step, "ex1_basic_micronMT29_WC1_tpcc_neworder", 100000, disk, table));
    }
}


[[maybe_unused]] static void ex_phd_extendend_batch(std::vector<std::future<bool>>& futures)
{
    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex2_extendend_samsungK9F1G_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex2_extendend_micronMT29AAA_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Warehouse();
        Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex2_extendend_micronMT29_WC1_tpcc_warehouse", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_NewOrder();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex2_extendend_samsungK9F1G_tpcc_neworder", disk, table));
    }

    {
        DBTable* table = new DBTable_TPCC_Customer();
        Disk* disk = new DiskFlashNandFTL_SamsungK9F1G08U0D();
        futures.push_back(DBThreadPool::threadPool.submit(ex_phd_extendend_step, "ex2_extendend_samsungK9F1G_tpcc_customer", disk, table));
    }
}


/**
 *
 * @brief Main PhD experiment functions for FA Tree
 *        By calling this function you will execute exeriments from PhD Thesis
 *
 */
void experimentPhdFATree()
{
    LOGGER_LOG_INFO("Starting FATree PHD experiments");

    std::filesystem::create_directories(FA_REAL_EXPERIMENTS_DIRECTORY_PATH);

    std::vector<std::future<bool>> futures;

    ex_phd_several_k_batch(futures);
    ex_phd_basic_batch(futures);
    ex_phd_extendend_batch(futures);

    LOGGER_LOG_INFO("FA PHD experiments {} job created", futures.size());
    for (size_t i = 0; i < futures.size(); ++i)
    {
        bool isComplete = futures[i].get();
        (void) isComplete;
    }

    LOGGER_LOG_INFO("Finished {} tasks from FA PHD experiments", futures.size());
}


