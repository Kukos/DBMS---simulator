#include <index/bbptree.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(bbptreeBasicSSDTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

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

    // specific for BBP
    EXPECT_EQ(bbp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, insertIntoBuffer)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;


    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, insertIntoTree)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    const double expectedEraseTime = ((143 + 36 + 1) / pagesInBlock) * eraseTime;
    // 143 touched leaves, 36 leaves split, 1 new inners
    const double expectedTime = 143 * (rReadTime + rWriteTime) + 36 * (rWriteTime + rReadTime + rWriteTime) + 1 * (rReadTime + rWriteTime + rReadTime + rWriteTime) + expectedEraseTime;

    EXPECT_NEAR(index->insertEntries(), expectedTime, 0.0001);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 2);

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    const double expectedEraseTime = ((143 + 36 + 1) / pagesInBlock) * eraseTime;
    // 143 touched leaves, 36 leaves split, 1 new inners
    const double expectedTime = 143 * (rReadTime + rWriteTime) + 36 * (rWriteTime + rReadTime + rWriteTime) + 1 * (rReadTime + rWriteTime + rReadTime + rWriteTime) + expectedEraseTime;

    EXPECT_NEAR(index->insertEntries(), expectedTime, 0.0001);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(bbp->getHeight(), 2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree* copy = new BBPTree(*dynamic_cast<BBPTree*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy->getHeight(), 2);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree copy2(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy2.getHeight(), 2);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy3.getHeight(), 2);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    delete copy;
    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), true);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    const double expectedEraseTime = ((143 + 36 + 1) / pagesInBlock) * eraseTime;
    // 143 touched leaves, 36 leaves split, 1 new inners
    const double expectedTime = 143 * (rReadTime + rWriteTime) + 36 * (rWriteTime + rReadTime + rWriteTime) + 1 * (rReadTime + rWriteTime + rReadTime + rWriteTime) + expectedEraseTime;

    EXPECT_NEAR(index->insertEntries(), expectedTime, 0.0001);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(bbp->getHeight(), 2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(index->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree* copy = new BBPTree(std::move(*dynamic_cast<BBPTree*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), true);

    EXPECT_EQ(copy->getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy->getHeight(), 2);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree copy2(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), true);

    EXPECT_EQ(copy2.getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy2.getHeight(), 2);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy2.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    BBPTree copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), true);

    EXPECT_EQ(copy3.getNumEntries(), numOperations + 1);
    EXPECT_EQ(copy3.getHeight(), 2);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 360);
    EXPECT_EQ(copy3.getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 440575);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 180);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 8192 * 180);

    delete copy;
    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, singleBulkload)
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

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 3);

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, severalBulkloads)
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

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

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

        EXPECT_EQ(bbp->getHeight(), 2);

        sumTime += expectedTime;
    }

    {
        const size_t diffInners = 0; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * sWriteTime + ((2.0 - 1.0) * rReadTime + rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated + dirtyPages) / pagesInBlock) * eraseTime;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime + sumTime, 0.0001);

        EXPECT_EQ(bbp->getHeight(), 2);

        sumTime += expectedTime;
    }

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, findPointInBuffer)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, findPointInBigTree)
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

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 3);

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

GTEST_TEST(bbptreeBasicSSDTest, findRangeInBuffer)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicSSDTest, findRangeInBigTree)
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

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 3);

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

GTEST_TEST(bbptreeBasicSSDTest, deleteFromBigTree)
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

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 100000;
    const size_t diffInners = 3; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * sWriteTime + (rReadTime + rWriteTime) * leavesCreated + (rReadTime + rWriteTime) * 2.0 * diffInners + ((diffInners + leavesCreated) / pagesInBlock) * eraseTime;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 3);

    const size_t numOperations = 10;
    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 0.0;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), entriesToInsert - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 3);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, interface)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

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

    // specific for BBP
    EXPECT_EQ(bbp->getHeight(), 0);

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, insertIntoBuffer)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, insertIntoTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = 4000 - 1;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    // 4000 touched leaves, 572 splits, 19 inners
    const double expectedTime = 4000 * (writeTime + writeTime + readTime) + 572 * (4.0 * writeTime + 4.0 * readTime + 4.0 * writeTime + writeTime + readTime) + 19 * (4.0 * readTime + 4.0 * writeTime +  4.0 * readTime + 4.0 * writeTime + writeTime + readTime);
    EXPECT_NEAR(index->insertEntries(), expectedTime, 0.00001);

    EXPECT_EQ(index->getNumEntries(), numOperations + 1);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTime, 0.00001);

    EXPECT_EQ(bbp->getHeight(), 3);

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, singleBulkload)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 4);

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, severalBulkloads)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    double sumTime = 0.0;
    {
        const size_t entriesToInsert = 10000;
        const size_t diffInners = 48; // hard to calculate so hardcoded
        const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
        const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

        EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

        EXPECT_EQ(bbp->getHeight(), 4);

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

        EXPECT_EQ(bbp->getHeight(), 4);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, findPointInBuffer)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1, 2), 0.0);


        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, findPointInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 3.0 * 3.0 * readTime + 6.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(static_cast<size_t>(5)), 5.0 * (3.0 * 3.0 * readTime + 6.0 * readTime));
        EXPECT_NEAR(index->findPointEntries(0.1), (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 6.0 * readTime), 0.0001);
        EXPECT_NEAR(index->findPointEntries(0.1, 2), 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 6.0 * readTime), 0.0001);

        sumTime += 3.0 * 3.0 * readTime + 6.0 * readTime + 5.0 * (3.0 * 3.0 * readTime + 6.0 * readTime) + (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 6.0 * readTime) + 2.0 * (entriesToInsert / 10) * (3.0 * 3.0 * readTime + 6.0 * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, findRangeInBuffer)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const size_t numOperations = 4000 - 1;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* index = bbp;

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 0);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.5, 2), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, findRangeInBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 4);

    double sumTime = 0.0;
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5)), 3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(5), 2), 2.0 * (3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(nodeSize / recordSize),  3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime);

        const size_t pagesToRead = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert / 2) / static_cast<double>((nodeSize / recordSize)))) * 8;
        EXPECT_NEAR(index->findRangeEntries(0.5),  3.0 * 3.0 * readTime + 6.0 * readTime + pagesToRead * readTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(0.5, 2), 2.0 * (3.0 * 3.0 * readTime + 6.0 * readTime + pagesToRead * readTime), 0.0001);

        sumTime +=  3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime + 2.0 * (3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime) +  3.0 * 3.0 * readTime + 6.0 * readTime + 8.0 * readTime + 3.0 * 3.0 * readTime + 6.0 * readTime + pagesToRead * readTime + 2.0 * (3.0 * 3.0 * readTime + 6.0 * readTime + pagesToRead * readTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(bbptreeBasicPCMTest, deleteFromBigTree)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 8;
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 100;

    BBPTree* bbp = new BBPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* index = bbp;

    const size_t entriesToInsert = 10000;
    const size_t diffInners = 48; // hard to calculate so hardcoded
    const size_t leavesCreated = static_cast<size_t>(ceil(static_cast<double>(entriesToInsert) / static_cast<double>((nodeSize / recordSize))));
    const double expectedTime = leavesCreated * writeTime * 8.0 + (5.0 * readTime + 5.0 * writeTime) * leavesCreated + (4.0 * readTime + 4.0 * writeTime + 5.0 * readTime + 5.0 * writeTime) * diffInners;

    EXPECT_NEAR(index->bulkloadEntries(entriesToInsert), expectedTime, 0.0001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedTime, 0.0001);

    EXPECT_EQ(bbp->getHeight(), 4);

    for (size_t i = 0; i < numOperations; ++i)
    {
        const double expectedTime = 0.0;
        EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTime);

        EXPECT_EQ(index->getNumEntries(), entriesToInsert - i - 1);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(bbp->getHeight(), 4);
    }

    delete index;
}
