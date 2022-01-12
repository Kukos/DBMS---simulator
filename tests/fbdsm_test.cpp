#include <index/fbdsm.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(fbdsmBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

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

GTEST_TEST(fbdsmBasicTest, topology)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numEntries = 1000000000;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(index->getNumEntries(), numEntries);

    delete index;
}


GTEST_TEST(fbdsmBasicTest, insertIntoBuffer)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 4000 - 1;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(fbdsmBasicTest, insertIntoDisk)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 4000 - 1;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;
    const double seqWrite = 15.3 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    // into buffer
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    // into disk
    double expectedTime = 0;
    for (size_t i = 0; i < numColumns; ++i)
    {
        const size_t touchedPages = (4000 * columns[i] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
        expectedTime += (touchedPages >= 4 ? seqWrite : writeTime ) * touchedPages;
    }

    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1));
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime);

    // into buffer
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime);
    }

    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

    EXPECT_EQ(index->getNumEntries(), 2 * (numOperations + 1));
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 2 * (numOperations + 1));
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 2.0 * expectedTime);

    delete index;
}

GTEST_TEST(fbdsmBasicTest, bulkload)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 100;
    const size_t numEntries = 500;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

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

GTEST_TEST(fbdsmBasicTest, copy)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const size_t recordSize = keySize + dataSize;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(1), numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM* copy = new FBDSM(*dynamic_cast<FBDSM*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM copy2(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    delete index;
    delete copy;
}


GTEST_TEST(fbdsmBasicTest, move)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const size_t recordSize = keySize + dataSize;
    const size_t numColumns = columns.size();
    const double writeTime = 45.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(1), numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM* copy = new FBDSM(std::move(*dynamic_cast<FBDSM*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM copy2(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    FBDSM copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, recordSize);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns* (writeTime));

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, numColumns);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 8192 * numColumns);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, numColumns * writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, numColumns* (writeTime));

    delete index;
    delete copy;
}

GTEST_TEST(fbdsmBasicTest, findPoint)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 10;
    const size_t numEntries = 50000;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

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

GTEST_TEST(fbdsmBasicTest, findRange)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 10;
    const size_t numEntries = 50000;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

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

GTEST_TEST(fbdsmBasicTest, deleteFromBuffer)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 4000 - 1;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(fbdsmBasicTest, deleteFromDisk)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t numOperations = 4000 - 1;
    const size_t numColumns = columns.size();
    const size_t pagesInBlock = ssd->getLowLevelController().getBlockSize() / ssd->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double seqWrite = 15.3 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;
    const double seqReadTime = 14.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    FBDSM* fbdsm = new FBDSM(ssd, columns);
    DBIndexColumn* index = fbdsm;

    // into buffer
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    // into disk
    double expectedTime = 0;
    for (size_t i = 0; i < numColumns; ++i)
    {
        const size_t touchedPages = (4000 * columns[i] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
        expectedTime += (touchedPages >= 4 ? seqWrite : writeTime ) * touchedPages;
    }

    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1));
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime);

    // into buffer
    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime);
    }

    size_t dirtyPageCounter = 0;
    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        if (i < 10)
        {
            EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);
        }
        else
        {
            const size_t keyPages = ((numOperations + 1- i) * columns[0] + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
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

            EXPECT_NEAR(index->deleteEntries(), expectedTime, 0.0001);
        }

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}