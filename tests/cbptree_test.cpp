#include <index/cbptree.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(cbptreeBasicSSDTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

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

    // specific for CBP
    EXPECT_EQ(cbp->getHeight(), 0);

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    CBPTree* bp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime + 5.0 * readTime);

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
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree* copy = new CBPTree(*dynamic_cast<CBPTree*>(index));
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
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree copy2(*copy);
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
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree copy3;
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
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    delete copy;
    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const double writeTime = 45.0 / 1000000.0;
    const double readTime = 21.0 / 1000000.0;

    CBPTree* bp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bp;

    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    EXPECT_DOUBLE_EQ(index->insertEntries(), writeTime + 5.0 * readTime);

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
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree* copy = new CBPTree(std::move(*dynamic_cast<CBPTree*>(index)));
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
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree copy2(std::move(*copy));
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
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    CBPTree copy3;
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
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, writeTime + 5.0 * readTime);

    delete copy;
    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, insertIntoRoot)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

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

        const double expectedTime = 4.0 * rReadTime + rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, insertToInner)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    size_t numEntries = 0;
    for (size_t j = 0; j < 4; ++j)
    {
        for (size_t i = 0; i < numOperations - (j != 0); ++i)
        {
            ++dirtyPageCounter;
            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = (j == 0 ? 0.0 : rReadTime) + 4.0 * rReadTime + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
            ++numEntries;

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1 + (j == 0 ? 0 : 1));
        }

        // Add Overflow node
        if (j < 3)
        {
            ++dirtyPageCounter;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = (j == 0 ? 0.0 : rReadTime) + 4.0 * rReadTime + rReadTime + rWriteTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else // flush Overflow nodes
        {
            dirtyPageCounter += 3;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = rReadTime + 4.0 * rReadTime + rReadTime + rWriteTime + rWriteTime + rReadTime + rWriteTime + rReadTime + rWriteTime + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
    }

    delete index;
}


GTEST_TEST(cbptreeBasicSSDTest, singleBulkload)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * (4.0 * rReadTime) + leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 3);

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, severalBulkloads)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 10000;
    double sumTime = 0.0;
    size_t dirtyPages;
    {
        const size_t diffInners = 1; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * (4.0 * rReadTime) + leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;
        dirtyPages = (diffInners + leavesCreated) % pagesInBlock;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(cbp->getHeight(), 2);

        sumTime += expectedTime;
    }

    {
        const size_t diffInners = 0; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * (4.0 * rReadTime) + leavesCreated * sWriteTime + ((2.0 - 1.0) * rReadTime + rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated + dirtyPages) / pagesInBlock) * eraseTime;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(cbp->getHeight(), 2);

        sumTime += expectedTime;
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, findPointInRoot)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 * rReadTime + rReadTime + rWriteTime + ((i > 1 && i % (pagesInBlock - 1) == 0) ? eraseTime : 0.0);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 4.0 * rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (4.0 * rReadTime + rReadTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * (4.0 * rReadTime + rReadTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * (4.0 * rReadTime + rReadTime), 0.0001);

        sumTime += (4.0 * rReadTime + rReadTime) + 5.0 * (4.0 * rReadTime + rReadTime) + (numOperations / 10) * (4.0 * rReadTime + rReadTime) + 2.0 * (numOperations / 10) * (4.0 * rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, findPointInBigTree)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * (4.0 * rReadTime) + leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 2.0 * rReadTime + (4.0 * rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime), 0.001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime), 0.001);

        sumTime += 2.0 * rReadTime + (4.0 * rReadTime + rReadTime) + 5.0 * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime) + (entriesToInsert / 10) * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime) + 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, findRangeInRoot)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 * rReadTime + rReadTime + rWriteTime + ((i > 1 && i % (pagesInBlock - 1) == 0) ? eraseTime : 0.0);
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), rReadTime + 4.0 * rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (rReadTime + 4.0 * rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 4.0 * rReadTime + 2.0 * rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), 4.0 * rReadTime + rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 2.0 * (4.0 * rReadTime + rReadTime + rReadTime));

        sumTime += rReadTime + 4.0 * rReadTime + rReadTime + 2.0 * (rReadTime + 4.0 * rReadTime + rReadTime) +  4.0 * rReadTime + 2.0 * rReadTime + 4.0 * rReadTime +  + rReadTime + rReadTime + 2.0 * (4.0 * rReadTime + rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, findRangeInBigTree)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * (4.0 * rReadTime) + leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 3);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 2.0 * rReadTime + 4.0 * rReadTime + rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (4.0 * rReadTime + 2.0 * rReadTime + rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 2.0 * rReadTime + 4.0 * rReadTime + 2.0 * rReadTime);
        EXPECT_NEAR(index->findRangeEntries(0.1), 2.0 * rReadTime + 4.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.1, 2), 2.0 * (2.0 * rReadTime + 4.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime), 0.0001);

        sumTime += 2.0 * rReadTime + 4.0 * rReadTime + rReadTime + rReadTime + 2.0 * (2.0 * rReadTime + 4.0 * rReadTime + rReadTime + rReadTime) + 2.0 * rReadTime + 4.0 * rReadTime + 2.0 * rReadTime +  2.0 * rReadTime + 4.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime + 2.0 * (2.0 * rReadTime + 4.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, deleteFromRoot)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

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

        const double expectedTime = 4.0 * rReadTime + rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
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

        const double expectedTime = 4.0 * rReadTime + rReadTime + rWriteTime + expectedEraseTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicSSDTest, deleteFromInner)
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

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    size_t dirtyPageCounter = 0;
    size_t numEntries = 0;
    for (size_t j = 0; j < 4; ++j)
    {
        for (size_t i = 0; i < numOperations - (j != 0); ++i)
        {
            ++dirtyPageCounter;
            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = (j == 0 ? 0.0 : rReadTime) + 4.0 * rReadTime + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
            ++numEntries;

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1 + (j == 0 ? 0 : 1));
        }

        // Add Overflow node
        if (j < 3)
        {
            ++dirtyPageCounter;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = (j == 0 ? 0.0 : rReadTime) + 4.0 * rReadTime + rReadTime + rWriteTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else // flush Overflow nodes
        {
            dirtyPageCounter += 3;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = rReadTime + 4.0 * rReadTime + rReadTime + rWriteTime + rWriteTime + rReadTime + rWriteTime + rReadTime + rWriteTime + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
    }

    sumTime = 0;
    size_t deleteOp = 0;
    for (size_t j = 0; j < 4; ++j)
    {
        if (j < 3)
        {
            ++dirtyPageCounter;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = 4.0 * rReadTime + rReadTime + rReadTime + rWriteTime + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else
        {
            dirtyPageCounter += 3;

            double expectedEraseTime = 0.0;
            if (dirtyPageCounter >= pagesInBlock)
            {
                expectedEraseTime = eraseTime * dirtyPageCounter / pagesInBlock;
                dirtyPageCounter = dirtyPageCounter % pagesInBlock;
            }

            const double expectedTime = 4.0 * rReadTime + rReadTime + 5.0 * rReadTime + 5.0 * rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1);
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

            const double expectedTime = 4.0 * rReadTime + (j == 3 ? 0.0 : rReadTime) + rReadTime + rWriteTime + expectedEraseTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), (j == 3 ? 1 : 2));
        }
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, interface)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

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

    // specific for CBP
    EXPECT_EQ(cbp->getHeight(), 0);

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, insertIntoRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, insertIntoInner)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    size_t numEntries = 0;

    for (size_t j = 0; j < 4; ++j)
    {
        for (size_t i = 0; i < numOperations - (j != 0); ++i)
        {
            const double expectedTime = (j == 0 ? 0.0 : 3.0 * readTime) + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
            sumTime += expectedTime;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
            ++numEntries;

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1 + (j == 0 ? 0 : 1));
        }

        // Add Overflow node
        if (j < 3)
        {
            const double expectedTime = (j == 0 ? 0.0 : 3.0 * readTime) + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime + 4.0 * writeTime;

            sumTime += expectedTime;
            ++numEntries;

            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else // flush Overflow nodes
        {
            const double expectedTime = 3.0 * readTime + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime + 4.0 * writeTime + 4.0 * readTime + 5.0 * writeTime + 4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5 * writeTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
    }


    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, singleBulkload)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (4.0 * readTime * leavesCreated) + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 4);

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, severalBulkloads)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 48; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = (4.0 * readTime * leavesCreated) + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(cbp->getHeight(), 4);

        sumTime += expectedTime;
    }

    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 46; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = (4.0 * readTime * leavesCreated) + (3.0 * readTime * (4.0 - 1.0)) * leavesCreated + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(cbp->getHeight(), 4);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, findPointInRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 *readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 4.0 * readTime + 3.0 * readTime);
        EXPECT_NEAR(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (4.0 * readTime + 3.0 * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1), (numOperations / 10) * (4.0 * readTime + 3.0 * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (numOperations / 10) * (4.0 * readTime + 3.0 * readTime), 0.0001);

        sumTime += 4.0 * readTime + 3.0 * readTime + 5.0 * (4.0 * readTime + 3.0 * readTime) + (numOperations / 10) * (4.0 * readTime + 3.0 * readTime) + 2.0 * (numOperations / 10) * (4.0 * readTime + 3.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, findPointInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (4.0 * readTime * leavesCreated) + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime), 0.0001);

        sumTime += 3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 5.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime) + (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime) + 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, findRangeInRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 4.0 * readTime + 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (4.0 * readTime + 3.0 * readTime + 8.0 * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 4.0 * readTime + 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5), 4.0 * readTime + 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5, 2), 2.0 * (4.0 * readTime + 3.0 * readTime + 8.0 * readTime));

        sumTime += 4.0 * readTime + 3.0 * readTime + 8.0 * readTime + 2.0 * (4.0 * readTime + 3.0 * readTime + 8.0 * readTime) + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime +  2.0 * (4.0 * readTime + 3.0 * readTime + 8.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, findRangeInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = cbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = (4.0 * readTime * leavesCreated) + leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(cbp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize),  3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime);

        const size_t pagesToRead = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert / 2) / static_cast<double>((nodeSize / recordSize)))) * 8;
        EXPECT_NEAR(index->findRangeEntries(0.5),  3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + pagesToRead * readTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.5, 2), 2.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + pagesToRead * readTime), 0.0001);

        sumTime +=  3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime + 2.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime) +  3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + 8.0 * readTime + 3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + pagesToRead * readTime + 2.0 * (3.0 * 3.0 * readTime + 4.0 * readTime + 3.0 * readTime + pagesToRead * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, deleteFromRoot)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    sumTime = 0.0;
    for (size_t i = 0; i < numOperations - 1; ++i)
    {
        const double expectedTime = 4.0 * readTime + 4.0 * readTime + 4.0 * writeTime;
        sumTime += expectedTime;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

        EXPECT_EQ(cbp->getHeight(), 1);
    }

    delete index;
}

GTEST_TEST(cbptreeBasicPCMTest, deleteFromInner)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = nodeSize / recordSize;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    CBPTree* cbp = new CBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = cbp;

    size_t numEntries = 0;
    double sumTime = 0.0;
    for (size_t j = 0; j < 4; ++j)
    {
        for (size_t i = 0; i < numOperations - (j != 0); ++i)
        {
            const double expectedTime = (j == 0 ? 0.0 : 3.0 * readTime) + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime;
            sumTime += expectedTime;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);
            ++numEntries;

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1 + (j == 0 ? 0 : 1));
        }

        // Add Overflow node
        if (j < 3)
        {
            const double expectedTime = (j == 0 ? 0.0 : 3.0 * readTime) + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime + 4.0 * writeTime;

            sumTime += expectedTime;
            ++numEntries;

            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else // flush Overflow nodes
        {
            const double expectedTime = 3.0 * readTime + 4.0 * readTime + 5.0 * readTime + 6.0 * writeTime + 4.0 * writeTime + 4.0 * readTime + 5.0 * writeTime + 4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5 * writeTime;
            sumTime += expectedTime;
            ++numEntries;
            EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numEntries);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
    }

    sumTime = 0;
    size_t deleteOp = 0;
    for (size_t j = 0; j < 4; ++j)
    {
        if (j < 3)
        {
            const double expectedTime = 3.0 * readTime + 4.0 * readTime + 4.0 * readTime + 4.0 * writeTime + 4.0 * readTime + 8.0 * writeTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 2);
        }
        else
        {
            const double expectedTime = 3.0 * readTime + 4.0 * readTime + 4.0 * readTime + 4.0 * writeTime + 4.0 * readTime + 8.0 * writeTime + 5.0 * readTime + 5.0 * writeTime + 5.0 * readTime + 8.0 * writeTime + 4.0 * readTime + 5.0 * writeTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), 1);
        }

        for (size_t i = 0; i < numOperations - 1; ++i)
        {
            const double expectedTime = 4.0 * readTime + (j == 3 ? 0.0 : 3.0 * readTime) + 4.0 * readTime + 4.0 * writeTime;
            sumTime += expectedTime;
            --numEntries;
            ++deleteOp;

            EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

            EXPECT_EQ(index->getNumEntries(), numEntries);

            EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, deleteOp);
            EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, sumTime);

            EXPECT_EQ(cbp->getHeight(), (j == 3 ? 1 : 2));
        }
    }

    delete index;
}