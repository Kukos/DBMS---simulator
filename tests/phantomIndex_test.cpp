#include <index/phantomIndex.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>


GTEST_TEST(phantomIndexBasicSSDTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 524288);
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
    EXPECT_EQ(index->getKeySize(), 0);
    EXPECT_EQ(index->getDataSize(), 0);
    EXPECT_EQ(index->getRecordSize(), 0);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 0;
    const size_t dataSize = 0;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new PhantomIndex(disk, true);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex* copy = new PhantomIndex(*dynamic_cast<PhantomIndex*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex copy2(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    delete copy;
    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 0;
    const size_t dataSize = 0;
    const size_t recordSize = keySize + dataSize;

    DBIndex* index = new PhantomIndex(disk, true);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex* copy = new PhantomIndex(std::move(*dynamic_cast<PhantomIndex*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumEntries(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex copy2(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumEntries(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    PhantomIndex copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumEntries(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

    delete copy;
    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, insert)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    const size_t numOperations = 1000;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, bulkload)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(100000), 0.0);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    delete index;
}


GTEST_TEST(phantomIndexBasicSSDTest, findPoint)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    const size_t numOperations = 1000;


    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + 100 + 200));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, findRange)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    const size_t numOperations = 1000;


    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 6);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicSSDTest, delete)
{
    Disk* disk = new DiskSSD_Samsung840();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    const size_t numOperations = 1000;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
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


GTEST_TEST(phantomIndexBasicPCMTest, interface)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("PCM:defaultModel"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 64);
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
    EXPECT_EQ(index->getKeySize(), 0);
    EXPECT_EQ(index->getDataSize(), 0);
    EXPECT_EQ(index->getRecordSize(), 0);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    delete index;
}

GTEST_TEST(phantomIndexBasicPCMTest, insert)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    const size_t numOperations = 1000;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicPCMTest, bulkload)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk, true);
    DBIndex* index = ph;

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(100000), 0.0);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    delete index;
}


GTEST_TEST(phantomIndexBasicPCMTest, findPoint)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;
    const size_t numOperations = 1000;


    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + 100 + 200));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicPCMTest, findRange)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    const size_t numOperations = 1000;


    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 6);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(phantomIndexBasicPCMTest, delete)
{
    Disk* disk = new DiskPCM_DefaultModel();

    PhantomIndex* ph = new PhantomIndex(disk);
    DBIndex* index = ph;

    const size_t numOperations = 1000;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
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