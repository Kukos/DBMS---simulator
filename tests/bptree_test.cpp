#include <index/bptree.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskFlashNandFTL.hpp>
#include <disk/diskFlashNandRaw.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(bptreeBasicSSDTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

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
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    // specific for BP
    EXPECT_EQ(bp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, topology)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numEntries = 1000000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(bp->getHeight(), 4);

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime + readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);
    EXPECT_EQ(bp->getHeight(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree* copy = new BPTree(*dynamic_cast<BPTree*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);
    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree copy2(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);
    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);
    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    delete copy;
    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime + readTime);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(index->getNumEntries(), 1);
    EXPECT_EQ(bp->getHeight(), 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree* copy = new BPTree(std::move(*dynamic_cast<BPTree*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy->getNumEntries(), 1);
    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree copy2(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy2.getNumEntries(), 1);
    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    BPTree copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 4168);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192);
    EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(copy3.getNumEntries(), 1);
    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1L);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second, 1L);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + readTime);

    delete copy;
    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, insertIntoRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, insertToInner)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 4.0 * rReadTime + 5.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // insert into leaf but with seeking root
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2.0 * rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner created before with seeking root
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 3.0 * rReadTime + 3.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    delete index;
}


GTEST_TEST(bptreeBasicSSDTest, singleBulkload)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, severalBulkloads)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    double sumTime = 0.0;
    size_t dirtyPages;
    {
        const size_t diffInners = 1; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;
        dirtyPages = (diffInners + leavesCreated) % pagesInBlock;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);

        sumTime += expectedTime;
    }

    {
        const size_t diffInners = 0; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * sWriteTime + ((2.0 - 1.0) * rReadTime + rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated + dirtyPages) / pagesInBlock) * eraseTime;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);

        sumTime += expectedTime;
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, findPointInRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = rReadTime + rWriteTime + ((i > 1 && i % (pagesInBlock - 1) == 0) ? eraseTime : 0.0);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), rReadTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * rReadTime);
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * rReadTime, 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * rReadTime, 0.0001);

        sumTime += rReadTime + 5.0 * rReadTime + (numOperations / 10) * rReadTime + 2.0 * (numOperations / 10) * rReadTime;

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, findPointInBigTree)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;
    size_t numOperations = 10;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 2.0 * rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (2.0 * rReadTime + rReadTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime), 0.001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime), 0.001);

        sumTime += 2.0 * rReadTime + rReadTime + 5.0 * (2.0 * rReadTime + rReadTime) + (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime) + 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, findRangeInRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = rReadTime + rWriteTime + ((i > 1 && i % (pagesInBlock - 1) == 0) ? eraseTime : 0.0);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 2.0 * rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 2.0 * (rReadTime + rReadTime));

        sumTime += rReadTime + rReadTime + 2.0 * (rReadTime + rReadTime) +  2.0 * rReadTime + rReadTime + rReadTime + 2.0 * (rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, findRangeInBigTree)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;
    size_t numOperations = 10;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 2.0 * rReadTime + rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (2.0 * rReadTime + rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 2.0 * rReadTime + 2.0 * rReadTime);
        EXPECT_NEAR(index->findRangeEntries(0.1), 2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.1, 2), 2.0 * (2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime), 0.0001);

        sumTime += 2.0 * rReadTime + rReadTime + rReadTime + 2.0 * (2.0 * rReadTime + rReadTime + rReadTime) + 2.0 * rReadTime + 2.0 * rReadTime +  2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime + 2.0 * (2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, deleteFromRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicSSDTest, deleteFromInner)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 4.0 * rReadTime + 5.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // insert into leaf but with seeking root
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2.0 * rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner created before with seeking root
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 3.0 * rReadTime + 3.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    sumTime = 0;
    // Delete entry from 3rd leaf, merge 3nd wqith 3rd leaf and notify changes in Inner level
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + 3.0 * rReadTime + 3.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // change heigh, so also merge inners
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = rReadTime + 5.0 * rReadTime + 5.0 * rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1 + numOperations - 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}


GTEST_TEST(bptreeBasicFlashFTLTest, interface)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("FlashNandFTL:micronMT29F32G08ABAAA"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 8192 * 128);
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

    // specific for BP
    EXPECT_EQ(bp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, topology)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numEntries = 1000000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(bp->getHeight(), 4);

    delete index;
}


GTEST_TEST(bptreeBasicFlashFTLTest, insertIntoRoot)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, insertIntoInner)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 4.0 * readTime + 5.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // insert into leaf but with seeking root
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2.0 * readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner created before with seeking root
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 3.0 * readTime + 3.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, singleBulkload)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime + (readTime + writeTime) * leavesCreated + (readTime + writeTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, severalBulkloads)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    double sumTime = 0.0;
    size_t dirtyPages;
    {
        const size_t diffInners = 1; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * writeTime + (readTime + writeTime) * leavesCreated + (readTime + writeTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;
        dirtyPages = (diffInners + leavesCreated) % pagesInBlock;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);

        sumTime += expectedTime;
    }

    {
        const size_t diffInners = 0; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * writeTime + ((2.0 - 1.0) * readTime + readTime + writeTime) * leavesCreated + (readTime + writeTime) * 2.0 * diffInners + ((diffInners + leavesCreated + dirtyPages) / pagesInBlock) * eraseTime;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);

        sumTime += expectedTime;
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, findPointInRoot)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * readTime);
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * readTime, 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * readTime, 0.0001);

        sumTime += readTime + 5.0 * readTime + (numOperations / 10) * readTime + 2.0 * (numOperations / 10) * readTime;

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, findPointInBigTree)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;
    const size_t numOperations = 10;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime + (readTime + writeTime) * leavesCreated + (readTime + writeTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 2.0 * readTime + readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (2.0 * readTime + readTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (2.0 * readTime + readTime), 0.001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (2.0 * readTime + readTime), 0.001);

        sumTime += 2.0 * readTime + readTime + 5.0 * (2.0 * readTime + readTime) + (entriesToInsert / 10) * (2.0 * readTime + readTime) + 2.0 * (entriesToInsert / 10) * (2.0 * readTime + readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, findRangeInRoot)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), readTime + readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (readTime + readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 2.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), readTime + readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 2.0 * (readTime + readTime));

        sumTime += readTime + readTime + 2.0 * (readTime + readTime) +  2.0 * readTime + readTime + readTime + 2.0 * (readTime + readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }


    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, findRangeInBigTree)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;
    const size_t numOperations = 10;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime + (readTime + writeTime) * leavesCreated + (readTime + writeTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 2.0 * readTime + readTime + readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (2.0 * readTime + readTime + readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 2.0 * readTime + 2.0 * readTime);
        EXPECT_NEAR(index->findRangeEntries(0.1), 2.0 * readTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * readTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.1, 2), 2.0 * (2.0 * readTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * readTime), 0.0001);

        sumTime += 2.0 * readTime + readTime + readTime + 2.0 * (2.0 * readTime + readTime + readTime) + 2.0 * readTime + 2.0 * readTime +  2.0 * readTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * readTime + 2.0 * (2.0 * readTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.01);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, deleteFromRoot)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashFTLTest, deleteFromInner)
{
    Disk* disk = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 4.0 * readTime + 5.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // insert into leaf but with seeking root
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 2.0 * readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner created before with seeking root
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = 3.0 * readTime + 3.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    sumTime = 0;
    // Delete entry from 3rd leaf, merge 3nd wqith 3rd leaf and notify changes in Inner level
    {
        dirtyPageCounter += 2;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + 3.0 * readTime + 3.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        ++dirtyPageCounter;
        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + readTime + writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // change heigh, so also merge inners
    {
        dirtyPageCounter += 3;

        double expectedEraseTime = 0.0;
        if (dirtyPageCounter >= pagesInBlock)
        {
            expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
            dirtyPageCounter = dirtyPageCounter % pagesInBlock;
        }

        const double expectedTime = readTime + 5.0 * readTime + 5.0 * writeTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1 + numOperations - 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, interface)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("FlashNandRaw:micronMT29F32G08ABAAA"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 8192 * 128);
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

    // specific for BP
    EXPECT_EQ(bp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, topology)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numEntries = 1000000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(bp->getHeight(), 4);

    delete index;
}


GTEST_TEST(bptreeBasicFlashRawTest, insertIntoRoot)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, insertIntoInner)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        const double expectedTime = (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (readTime * pagesInBlock / 2) + (writeTime * pagesInBlock) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, singleBulkload)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 1; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (leavesCreated + (pagesInBlock - 1) / pagesInBlock) * pagesInBlock * writeTime + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime) * leavesCreated + (readTime * (pagesInBlock / 2) + writeTime * pagesInBlock + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime)) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 2);

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, severalBulkloads)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    double sumTime = 0.0;
    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 1; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = (leavesCreated + (pagesInBlock - 1) / pagesInBlock) * pagesInBlock * writeTime + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime) * leavesCreated + (readTime * (pagesInBlock / 2) + writeTime * pagesInBlock + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime)) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);

        sumTime += expectedTime;
    }

    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 0; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime =  static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime * leavesCreated + (leavesCreated + (pagesInBlock - 1) / pagesInBlock) * pagesInBlock * writeTime + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime) * leavesCreated + (readTime * (pagesInBlock / 2) + writeTime * pagesInBlock + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime)) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, findPointInRoot)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime);
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime, 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime, 0.0001);

        sumTime +=  static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + 5.0 * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + (numOperations / 10) * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + 2.0 * (numOperations / 10) * static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime;

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, findPointInBigTree)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;
    const size_t numOperations = 100;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 1; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (leavesCreated + (pagesInBlock - 1) / pagesInBlock) * pagesInBlock * writeTime + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime) * leavesCreated + (readTime * (pagesInBlock / 2) + writeTime * pagesInBlock + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime)) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 2);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime), 0.0001);

        sumTime +=   static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime
                   + 5.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime)
                   + (entriesToInsert / 10) * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime)
                   +  2.0 * (entriesToInsert / 10) * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, findRangeInRoot)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime));

        sumTime +=  static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime
                  + 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime)
                  + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime
                  + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime
                  + 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, findRangeInBigTree)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;
    const size_t numOperations = 100;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 1; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (leavesCreated + (pagesInBlock - 1) / pagesInBlock) * pagesInBlock * writeTime + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime) * leavesCreated + (readTime * (pagesInBlock / 2) + writeTime * pagesInBlock + (readTime * pagesInBlock + writeTime * pagesInBlock + eraseTime)) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 2);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime);

        const size_t pagesToRead = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert / 10) / static_cast<double>((nodeSize / recordSize)))) * pagesInBlock;
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesToRead * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesToRead * readTime));

        sumTime +=   static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime
                   + 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime)
                   + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime
                   + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesToRead * readTime
                   + 2.0 * (static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesToRead * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, deleteFromRoot)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicFlashRawTest, deleteFromInner)
{
    Disk* disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    // since overwrite is time costly to simulate lets make big record to reduce numOfOperations
    const size_t dataSize = 6400;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getBlockSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 35.0 / 1000000.0;
    const double writeTime = 350.0 / 1000000.0;
    const double eraseTime = 1500.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        const double expectedTime = (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (readTime * pagesInBlock / 2) + (writeTime * pagesInBlock) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    sumTime = 0;
    // Delete entry from 3rd leaf, merge 3nd wqith 3rd leaf and notify changes in Inner level
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock / 2 * readTime + pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + pagesInBlock * readTime + (pagesInBlock * writeTime) + eraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // change heigh, so also merge inners
    {
        const double expectedTime = static_cast<size_t>(floor(log(static_cast<double>(pagesInBlock)) / log(2.0))) * readTime + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock / 2 * readTime + pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime) + (pagesInBlock / 2 * readTime + pagesInBlock * writeTime) + (pagesInBlock * readTime + pagesInBlock * writeTime + eraseTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1 + numOperations - 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, interface)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

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
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    // specific for BP
    EXPECT_EQ(bp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, topology)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numEntries = 1000000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(bp->getHeight(), 7);

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, insertIntoRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, insertIntoInner)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        const double expectedTime = (5.0 * readTime + 6.0 * writeTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime) + (4.0 * readTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = 3.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);

        // need to use expect near, because of double arytmetic rounding error
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner
    {
        const double expectedTime = (3.0 * readTime) + (5.0 * readTime + 6.0 * writeTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, singleBulkload)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 4);

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, severalBulkloads)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    double sumTime = 0.0;
    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 48; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 4);

        sumTime += expectedTime;
    }

    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 46; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = (3.0 * readTime * (4.0 - 1.0)) * leavesCreated + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 4);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, findPointInRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 3.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * 3.0 * readTime);
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * 3.0 * readTime, 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * 3.0 * readTime, 0.0001);

        sumTime += 3.0 * readTime + 5.0 * 3.0 * readTime + (numOperations / 10) * 3.0 * readTime + 2.0 * (numOperations / 10) * 3.0 * readTime;

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, findPointInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 3.0 * 3.0 * readTime + 3.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (3.0 * 3.0 * readTime + 3.0 * readTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 3.0 * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 3.0 * readTime), 0.0001);

        sumTime += 3.0 * 3.0 * readTime + 3.0 * readTime + 5.0 * (3.0 * 3.0 * readTime + 3.0 * readTime) + (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 3.0 * readTime) + 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 3.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, findRangeInRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (3.0 * readTime + 8.0 * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5), 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5, 2), 2.0 * (3.0 * readTime + 8.0 * readTime));

        sumTime += 3.0 * readTime + 8.0 * readTime + 2.0 * (3.0 * readTime + 8.0 * readTime) + 3.0 * readTime + 8.0 * readTime + 3.0 * readTime + 8.0 * readTime +  2.0 * (3.0 * readTime + 8.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, findRangeInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize),  3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime);

        const size_t pagesToRead = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert / 2) / static_cast<double>((nodeSize / recordSize)))) * 8;
        EXPECT_NEAR(index->findRangeEntries(0.5),  3.0 * 3.0 * readTime + 3.0 * readTime + pagesToRead * readTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.5, 2), 2.0 * (3.0 * 3.0 * readTime + 3.0 * readTime + pagesToRead * readTime), 0.0001);

        sumTime +=  3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime + 2.0 * (3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime) +  3.0 * 3.0 * readTime + 3.0 * readTime + 8.0 * readTime + 3.0 * 3.0 * readTime + 3.0 * readTime + pagesToRead * readTime + 2.0 * (3.0 * 3.0 * readTime + 3.0 * readTime + pagesToRead * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, deleteFromRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = 4.0 * readTime + 4.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(bptreeBasicPCMTest, deleteFromInner)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }

    // Insert into Inner
    {
        const double expectedTime = (5.0 * readTime + 6.0 * writeTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime) + (4.0 * readTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = 3.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);

        // need to use expect near, because of double arytmetic rounding error
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // Insert into Inner
    {
        const double expectedTime = (3.0 * readTime) + (5.0 * readTime + 6.0 * writeTime) + (4.0 * writeTime) + (5.0 * readTime + 5.0 * writeTime);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + numOperations);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime, 0.0001);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    sumTime = 0;
    // Delete entry from 3rd leaf, merge 3nd wqith 3rd leaf and notify changes in Inner level
    {
        const double expectedTime = 3.0 * readTime + 4.0 * readTime + 4.0 * writeTime + 4.0 * readTime + 8.0 * writeTime + 5.0 * readTime + 5.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = 3.0 * readTime + 4.0 * readTime + 4.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 2);
    }

    // change heigh, so also merge inners
    {
        const double expectedTime = 3.0 * readTime + 4.0 * readTime + 4.0 * writeTime + 4.0 * readTime + 8.0 * writeTime + 5.0 * readTime + 5.0 * writeTime + 4.0 * readTime + 8.0 * writeTime + 5.0 * readTime + 5.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations + numOperations - numOperations);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 1 + numOperations - 1 + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(bp->getHeight(), 1);
    }


    delete index;
}