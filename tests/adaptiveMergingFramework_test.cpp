#include <adaptiveMerging/adaptiveMergingFramework.hpp>
#include <index/dbIndex.hpp>
#include <storage/memoryModelPCM.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

class MemoryControllerTest : public MemoryController
{
private:
    double readTime;
    double writeTime;
    size_t writeQueueSize;
    size_t overwriteQueueSize;

    size_t bytesToPages(size_t bytes) const noexcept(true)
    {
        const size_t pageSize = memoryModel->getPageSize();
        return (bytes + (pageSize - 1)) / pageSize;
    }

public:
    ~MemoryControllerTest() = default;
    MemoryControllerTest() = default;
    MemoryControllerTest(const MemoryControllerTest&) = default;
    MemoryControllerTest& operator=(const MemoryControllerTest&) = default;
    MemoryControllerTest(MemoryControllerTest &&) = default;
    MemoryControllerTest& operator=(MemoryControllerTest &&) = default;

    MemoryControllerTest(const char* modelName,
                         size_t pageSize,
                         double readTime,
                         double writeTime)
    : MemoryController(new MemoryModelPCM(modelName, pageSize, readTime, writeTime)), readTime{readTime}, writeTime{writeTime}, writeQueueSize{0}, overwriteQueueSize{0}
    {

    }

    double flushCache() noexcept(true)
    {
        const double writeTime = writeQueueSize * this->writeTime;
        const double overwriteTime = overwriteQueueSize * this->writeTime;

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, writeTime);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, overwriteTime);

        writeQueueSize = 0;
        overwriteQueueSize = 0;

        return writeTime + overwriteTime;
    }

    double readBytes(uintptr_t addr, size_t bytes) noexcept(true) override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = static_cast<double>(pages) * readTime;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, time);

        return time;
    }

    double writeBytes(uintptr_t addr, size_t bytes) noexcept(true) override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = 0.0;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, time);

        writeQueueSize += pages;

        return time;
    }

    double overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true)  override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = 0.0;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, time);

        overwriteQueueSize += pages;
        return time;
    }
};

class DBIndexTest : public DBIndex
{
private:
    double findPointEntriesHelper(size_t numOperations = 1)  noexcept(true)
    {
        double time = 0.0;
        for (size_t i = 0; i < numOperations; ++i)
        {
            uintptr_t addr = disk->getCurrentMemoryAddr();

            // scan index (only keys)
            time += disk->readBytes(addr, this->numEntries * sizeKey);

            // go to the random place and seek record
            // in tests do not care about reality :)
            addr += 100 * sizeRecord;
            time += disk->readBytes(addr, sizeData);

            time += disk->flushCache();
        }

        return time;
    }
public:
    DBIndex* clone() const noexcept(true) override
    {
        return new DBIndexTest(*this);
    }

    double insertEntries(size_t numOperations = 1) noexcept(true) override
    {
        double time = 0.0;
        for (size_t i = 0; i < numOperations; ++i)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();

            time += disk->writeBytes(addr, sizeRecord);
            time += disk->flushCache();
        }

        numEntries += numOperations;

        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

        return time;
    }

    double bulkloadEntries(size_t numEntries = 1) noexcept(true) override
    {
        (void)numEntries;

        // unsupported
        return 0.0;
    }

    double deleteEntries(size_t numOperations = 1) noexcept(true) override
    {
        double time = 0.0;
        for (size_t i = 0; i < numOperations; ++i)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();

            time += findPointEntriesHelper();
            time += disk->overwriteBytes(addr, sizeRecord);
            time += disk->flushCache();
        }

        numEntries -= numOperations;

        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, numOperations);

        return time;
    }

    double findPointEntries(size_t numOperations = 1)  noexcept(true) override
    {
        const double time = findPointEntriesHelper(numOperations);

        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

        return time;
    }

    double findPointEntries(double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
    }

    double findRangeEntries(size_t numEntries, size_t numOperations = 1)  noexcept(true) override
    {
        double time = 0.0;
        for (size_t i = 0; i < numOperations; ++i)
        {
            uintptr_t addr = disk->getCurrentMemoryAddr();

            // scan index (only keys)
            time += disk->readBytes(addr, this->numEntries * sizeKey);

            // in random place scan entries (in tests do not care about reality :))
            addr += (this->numEntries / 2) * sizeRecord;
            time += disk->readBytes(addr, sizeData * numEntries);
            time += disk->flushCache();
        }

        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

        return time;
    }

    double findRangeEntries(double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
    }

    void createTopologyAfterInsert(size_t numEntries) noexcept(true)
    {
        this->numEntries += numEntries;
    }

    DBIndexTest(Disk* disk, size_t sizeKey, size_t sizeData)
    : DBIndex("Index", disk, sizeKey, sizeData)
    {

    }

    ~DBIndexTest() = default;
    DBIndexTest() = default;
    DBIndexTest(const DBIndexTest&) = default;
    DBIndexTest& operator=(const DBIndexTest&) = default;
    DBIndexTest(DBIndexTest &&) = default;
    DBIndexTest& operator=(DBIndexTest &&) = default;
};


class AMTest : public AdaptiveMergingFramework
{

public:
    AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new AMTest(*this);
    }

    double insertEntries(size_t numOperations = 1) noexcept(true) override
    {
        return index->insertEntries(numOperations);
    }

    double bulkloadEntries(size_t numEntries = 1) noexcept(true) override
    {
        return index->bulkloadEntries(numEntries);
    }

    double deleteEntries(size_t numOperations = 1) noexcept(true) override
    {
        (void)numOperations;
        return 0.0;
    }

    double findPointEntries(size_t numOperations = 1)  noexcept(true) override
    {
        (void)numOperations;
        return 0.0;
    }

    double findPointEntries(double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
    }

    double findRangeEntries(size_t numEntries, size_t numOperations = 1)  noexcept(true) override
    {
        (void)numEntries;
        (void)numOperations;
        return 0.0;
    }

    double findRangeEntries(double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
    }

    void createTopologyAfterInsert(size_t numEntries) noexcept(true)
    {
        index->createTopologyAfterInsert(numEntries);
    }

    double invalidEntries(Disk* disk, size_t numEntries, size_t nodeSize) noexcept(true)
    {
        return memoryManager->invalidEntries(disk, numEntries, nodeSize);
    }

    std::pair<size_t, size_t> splitLoadForIndexAndUnsortedPartTest(size_t numEntries) noexcept(true)
    {
        return splitLoadForIndexAndUnsortedPart(numEntries);
    }

    AMTest(DBIndex* index, AMUnsortedMemoryManager* manager, size_t startingEntries)
    : AdaptiveMergingFramework("AMTest", index, manager, startingEntries)
    {

    }

    ~AMTest() = default;
    AMTest() = default;
    AMTest(const AMTest&) = default;
    AMTest& operator=(const AMTest&) = default;
    AMTest(AMTest &&) = default;
    AMTest& operator=(AMTest &&) = default;
};

GTEST_TEST(adaptiveMergingFramework, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new DBIndexTest(new Disk(std::move(disk)), keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(10000, recordSize, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    DBIndex* am = new AMTest(index, manager, startingEntries);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getDisk().getLowLevelController().getModelName()), std::string(modelName));
    EXPECT_EQ(std::string(am->getName()), std::string("AMTest"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getPageSize(), pageSize);
    EXPECT_EQ(am->getDisk().getLowLevelController().getBlockSize(), 0);
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(am)->getMemoryManager().getNumEntries(), startingEntries);

    DBIndex* copy = am->clone();

    delete copy;
    delete am;
}


GTEST_TEST(adaptiveMergingFramework, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new DBIndexTest(new Disk(std::move(disk)), keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    DBIndex* am = new AMTest(index, manager, startingEntries);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getDisk().getLowLevelController().getModelName()), std::string(modelName));
    EXPECT_EQ(std::string(am->getName()), std::string("AMTest"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getPageSize(), pageSize);
    EXPECT_EQ(am->getDisk().getLowLevelController().getBlockSize(), 0);
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(am)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest* copy = new AMTest(*dynamic_cast<AMTest*>(am));
    EXPECT_EQ(copy->getNumEntries(), startingEntries);
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(copy)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest copy2(*copy);
    EXPECT_EQ(copy2.getNumEntries(), startingEntries);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getNumEntries(), startingEntries);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(&copy3)->getMemoryManager().getInvalidationType(), AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);

    delete copy;
    delete am;
}

GTEST_TEST(adaptiveMergingFramework, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new DBIndexTest(new Disk(std::move(disk)), keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    DBIndex* am = new AMTest(index, manager, startingEntries);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getDisk().getLowLevelController().getModelName()), std::string(modelName));
    EXPECT_EQ(std::string(am->getName()), std::string("AMTest"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getPageSize(), pageSize);
    EXPECT_EQ(am->getDisk().getLowLevelController().getBlockSize(), 0);
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(am)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest* copy = new AMTest(std::move(*dynamic_cast<AMTest*>(am)));
    EXPECT_EQ(copy->getNumEntries(), startingEntries);
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(copy)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest copy2(std::move(*copy));
    EXPECT_EQ(copy2.getNumEntries(), startingEntries);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);

    AMTest copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getNumEntries(), startingEntries);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(dynamic_cast<AdaptiveMergingFramework*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);

    delete copy;
    delete am;
}

GTEST_TEST(adaptiveMergingFramework, splitEntriesToLoad)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new DBIndexTest(disk.clone(), keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    DBIndex* am = new AMTest(index, manager, startingEntries);

    {
        std::pair<size_t, size_t> entriesToLoad = dynamic_cast<AMTest*>(am)->splitLoadForIndexAndUnsortedPartTest(100);
        EXPECT_EQ(entriesToLoad.first, 0);
        EXPECT_EQ(entriesToLoad.second, 100);
    }

    {
        am->createTopologyAfterInsert(startingEntries);
        std::pair<size_t, size_t> entriesToLoad = dynamic_cast<AMTest*>(am)->splitLoadForIndexAndUnsortedPartTest(100);
        EXPECT_EQ(entriesToLoad.first + entriesToLoad.second, 100);
        EXPECT_GT(entriesToLoad.first, 0);
        EXPECT_GT(entriesToLoad.second, 0);
    }

    DBIndex* index2 = new DBIndexTest(disk.clone(), keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager2 = new AdaptiveMergingFramework::AMUnsortedMemoryManager(0, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    DBIndex* am2 = new AMTest(index2, manager2, 0);

    {
        std::pair<size_t, size_t> entriesToLoad = dynamic_cast<AMTest*>(am2)->splitLoadForIndexAndUnsortedPartTest(100);
        EXPECT_EQ(entriesToLoad.first, 0);
        EXPECT_EQ(entriesToLoad.second, 0);
    }

    {
        am2->createTopologyAfterInsert(startingEntries);
        std::pair<size_t, size_t> entriesToLoad = dynamic_cast<AMTest*>(am2)->splitLoadForIndexAndUnsortedPartTest(100);
        EXPECT_EQ(entriesToLoad.first, 100);
        EXPECT_EQ(entriesToLoad.second, 0);
    }

    delete am;
    delete am2;
}

GTEST_TEST(adaptiveMergingFramework, invalidOverwriteSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        dirtyPageCounter += 2;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2 * rReadTime + 2 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, i % 10 + 1, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}


GTEST_TEST(adaptiveMergingFramework, invalidFlagSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_FLAG);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        dirtyPageCounter += 2;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2 * rReadTime + 2 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, (disk->getLowLevelController().getPageSize() / recordSize) * 2 - i % 10 + 1, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}

GTEST_TEST(adaptiveMergingFramework, invalidBitmapSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_BITMAP);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        dirtyPageCounter += 2;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2 * rReadTime + 2 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, (nodeSize / recordSize) * 2 - i % 10, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}

GTEST_TEST(adaptiveMergingFramework, invalidJournalSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        dirtyPageCounter += 1;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime =  1 * rReadTime + 1 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, (disk->getLowLevelController().getPageSize() / recordSize) * 2 - i % 10 + 1, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}


GTEST_TEST(adaptiveMergingFramework, invalidOverwritePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 2 * readTime + 2 * writeTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, 2, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}

GTEST_TEST(adaptiveMergingFramework, invalidFlagPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_FLAG);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 2 * readTime + 2 * writeTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, 2, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}

GTEST_TEST(adaptiveMergingFramework, invalidBitmapPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_BITMAP);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 1 * readTime + 1 * writeTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, 2, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}

GTEST_TEST(adaptiveMergingFramework, invalidJournalPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t numOperations = 100;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new DBIndexTest(disk, keySize, dataSize);
    AdaptiveMergingFramework::AMUnsortedMemoryManager* manager = new AdaptiveMergingFramework::AMUnsortedMemoryManager(startingEntries, recordSize,  AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL);
    AMTest* am = new AMTest(index, manager, startingEntries);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 1 * readTime + 1 * writeTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(am->invalidEntries(disk, 100, nodeSize), expectedTime);
        EXPECT_DOUBLE_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME), sumTime);
        EXPECT_EQ(am->getMemoryManager().getCounters().getCounterValue(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS), i + 1);

        EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, sumTime);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i + 1);
    }

    delete am;
}