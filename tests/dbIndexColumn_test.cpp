#include <index/dbIndexColumn.hpp>
#include <storage/memoryModelPCM.hpp>
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

class DBIndexColumnTest : public DBIndexColumn
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
    DBIndexColumn* clone() const noexcept(true) override
    {
        return new DBIndexColumnTest(*this);
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

    double findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations = 1)  noexcept(true) override
    {
        const double time = findPointEntriesHelper(numOperations * (columnsToFetch.size() - 1));

        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
        counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

        return time;
    }

    double findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findPointEntries(columnsToFetch, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
    }

    double findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations = 1)  noexcept(true) override
    {
        double time = 0.0;
        for (size_t i = 0; i < numOperations * (columnsToFetch.size() - 1); ++i)
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

    double findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) override
    {
        return findRangeEntries(columnsToFetch, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
    }

    void createTopologyAfterInsert(size_t numEntries) noexcept(true)
    {
        this->numEntries += numEntries;
    }

    DBIndexColumnTest(Disk* disk, const std::vector<size_t>& columnsSize)
    : DBIndexColumn("ColumnIndex", disk, columnsSize)
    {

    }

    ~DBIndexColumnTest() = default;
    DBIndexColumnTest() = default;
    DBIndexColumnTest(const DBIndexColumnTest&) = default;
    DBIndexColumnTest& operator=(const DBIndexColumnTest&) = default;
    DBIndexColumnTest(DBIndexColumnTest &&) = default;
    DBIndexColumnTest& operator=(DBIndexColumnTest &&) = default;
};

GTEST_TEST(generalDBIndexColumnBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getName()), std::string("ColumnIndex"));
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string(modelName));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), pageSize);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 0);
    EXPECT_EQ(index->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(index->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(index->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(index->getCounter(id).second, 0L);

    EXPECT_EQ(index->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    DBIndexColumn* copy = index->clone();

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    delete copy;
    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, insertEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    DBIndexColumnTest* copy = new DBIndexColumnTest(*dynamic_cast<DBIndexColumnTest*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);


    DBIndexColumnTest copy2 = DBIndexColumnTest(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    DBIndexColumnTest copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    delete copy;
    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    DBIndexColumnTest* copy = new DBIndexColumnTest(std::move(*dynamic_cast<DBIndexColumnTest*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);


    DBIndexColumnTest copy2 = DBIndexColumnTest(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    DBIndexColumnTest copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    delete copy;
    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, bulkloadEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(), 0);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 0L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 0L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, findPointEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}), 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, recordSize + recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, recordSize + recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 2L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 4.0 * readTime);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, findPointSelEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(2), 2.0 * writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 2L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 2L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getNumEntries(), 2);

    EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3, 4}, 0.5), 6.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 6);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 3 * (2 * keySize + dataSize));
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 6.0 * readTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 6);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 3 * (2 * keySize + dataSize));
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 6.0 * readTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 3L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 6.0 * readTime);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, findRangeEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 1.0 * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(1)), 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 2L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 4.0 * readTime);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, findRangeSelEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(2), 2.0 * writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 2 * recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 2L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 2L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 2.0 * writeTime);

    EXPECT_EQ(index->getNumEntries(), 2);

    EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.5), 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 2 * (2 * keySize + dataSize));
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 2 * (2 * keySize + dataSize));
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 3L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 4.0 * readTime);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, deleteEntry)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 1.0 * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(index->deleteEntries(), writeTime + 2.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0 * readTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0 * readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 2L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 1.0 * writeTime + 2.0 * readTime);

    EXPECT_EQ(index->getNumEntries(), 0);

    delete index;
}

GTEST_TEST(generalDBIndexColumnBasicTest, resetState)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t keySize = 8;
    const size_t dataSize = 10 + 20 + 50 + 100;
    const size_t recordSize = keySize + dataSize;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime);

    index->resetState();

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    delete index;
}


GTEST_TEST(generalDBIndexColumnBasicTest, topology)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);
    Disk disk(memoryController);

    const size_t entriesToInsert = 100000;

    std::vector<size_t> columns = {8, 10, 20, 50, 100};
    DBIndexColumn* index = new DBIndexColumnTest(new Disk(std::move(disk)), columns);

    index->createTopologyAfterInsert(entriesToInsert);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getNumEntries(), entriesToInsert);

    delete index;
}