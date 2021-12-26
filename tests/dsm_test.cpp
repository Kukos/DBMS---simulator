#include <index/dsm.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(dsmBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    // check how to get some statistics
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
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    delete index;
}


GTEST_TEST(dsmBasicTest, insert)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 1000;
    const size_t numColumns = columns.size();
    const size_t pagesInBlock = ssd->getLowLevelController().getBlockSize() / ssd->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        dirtyPageCounter += numColumns;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * (dirtyPageCounter / pagesInBlock);
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = (numColumns * (writeTime + readTime)) + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);
    }

    delete index;
}

GTEST_TEST(dsmBasicTest, bulkload)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 100;
    const size_t numEntries = 500;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        size_t touchedPages = 0;
        for (size_t j = 0; j < numColumns; ++j)
            touchedPages += (numEntries * columns[j] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();

        const double expectedTime = (touchedPages * writeTime);
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(index->bulkloadEntries(numEntries), expectedTime);

        EXPECT_EQ(index->getNumEntries(), (i + 1) * numEntries);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, sumTime);
    }

    delete index;
}

GTEST_TEST(dsmBasicTest, copy)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const size_t recordSize = keySize + dataSize;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->insertEntries(), numColumns* (writeTime + readTime));

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM* copy = new DSM(*dynamic_cast<DSM*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM copy2(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    delete index;
    delete copy;
}


GTEST_TEST(dsmBasicTest, move)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const size_t recordSize = keySize + dataSize;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->insertEntries(), numColumns* (writeTime + readTime));

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM* copy = new DSM(std::move(*dynamic_cast<DSM*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM copy2(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    DSM copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, numColumns* (writeTime + readTime));

    delete index;
    delete copy;
}

GTEST_TEST(dsmBasicTest, findPoint)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 10;
    const size_t numEntries = 50000;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    EXPECT_GT(index->bulkloadEntries(numEntries), 0.0);

    EXPECT_EQ(index->getNumEntries(), numEntries);

    const size_t keyPages = (numEntries * columns[0] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
    const double findKeyTime = (keyPages >= 4 ? seqReadTime : readTime) * keyPages;

    const std::vector<size_t> columnsToFetch {3, 0, 2};
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double singleSearchTime =  findKeyTime + (readTime * (columnsToFetch.size() - 1));
        EXPECT_DOUBLE_EQ(index->findPointEntries(columnsToFetch), singleSearchTime);
        EXPECT_NEAR(index->findPointEntries(columnsToFetch, 0.1), static_cast<size_t>(0.1 * numEntries) * singleSearchTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.1 * numEntries)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, (i + 1) * (static_cast<size_t>(0.1 * numEntries) + 1) * singleSearchTime, 0.0001);
    }

    delete index;
}

GTEST_TEST(dsmBasicTest, findRange)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 10;
    const size_t numEntries = 50000;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    EXPECT_GT(index->bulkloadEntries(numEntries), 0.0);

    EXPECT_EQ(index->getNumEntries(), numEntries);

    const size_t keyPages = (numEntries * columns[0] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
    const double findKeyTime = (keyPages >= 4 ? seqReadTime : readTime) * keyPages;

    const std::vector<size_t> columnsToFetch {3, 0, 2};
    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double singleSearchTime =  findKeyTime + (readTime * (columnsToFetch.size() - 1));
        sumTime += singleSearchTime;
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, static_cast<size_t>(1)), singleSearchTime);

        double expectedTime = 0.0;
        for (size_t j = 0; j < columnsToFetch.size(); ++j)
        {
            if (columnsToFetch[j] == 0)
                continue;

            const size_t touchedPages = (static_cast<size_t>(0.1 * numEntries) * columns[columnsToFetch[j]] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
            expectedTime += (touchedPages >= 4 ? seqReadTime : readTime) * touchedPages;
        }
        sumTime += expectedTime;
        EXPECT_NEAR(index->findRangeEntries(columnsToFetch, 0.1), expectedTime, 0.001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.01);
    }

    delete index;
}

GTEST_TEST(dsmBasicTest, delete)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 1000;
    const size_t numEntries = 50000;
    const size_t numColumns = columns.size();
    const size_t pagesInBlock = ssd->getLowLevelController().getBlockSize() / ssd->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    DSM* dsm = new DSM(ssd, columns);
    DBIndexColumn* index = dsm;

    EXPECT_GT(index->bulkloadEntries(numEntries), 0.0);

    EXPECT_EQ(index->getNumEntries(), numEntries);

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const size_t keyPages = ((numEntries - i) * columns[0] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
        const double findKeyTime = (keyPages >= 4 ? seqReadTime : readTime) * keyPages;

        dirtyPageCounter += numColumns;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * (dirtyPageCounter / pagesInBlock);
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = findKeyTime + (numColumns * (writeTime + readTime)) + expectedEraseTime;
        sumTime += expectedTime;

        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numEntries - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}
