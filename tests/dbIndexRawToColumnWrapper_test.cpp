#include <index/dbIndexRawToColumnWrapper.hpp>
#include <index/fdtree.hpp>
#include <index/bptree.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

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

    DBIndexRawToColumnWrapper* copy = new DBIndexRawToColumnWrapper(*dynamic_cast<DBIndexRawToColumnWrapper*>(index));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getNumEntries(), numOperations);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    DBIndexRawToColumnWrapper copy2 = DBIndexRawToColumnWrapper(*copy);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getNumEntries(), numOperations);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    DBIndexRawToColumnWrapper copy3;
    copy3 = copy2;
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getNumEntries(), numOperations);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    delete copy;
    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_EQ(index->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(index->getColumnSize(i), columns[i]);

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

    DBIndexRawToColumnWrapper* copy = new DBIndexRawToColumnWrapper(std::move(*dynamic_cast<DBIndexRawToColumnWrapper*>(index)));
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy->getColumnSize(i), columns[i]);

    EXPECT_EQ(copy->getNumEntries(), numOperations);

    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    DBIndexRawToColumnWrapper copy2 = DBIndexRawToColumnWrapper(std::move(*copy));
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy2.getNumEntries(), numOperations);

    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    DBIndexRawToColumnWrapper copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getNumOfColumns(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        EXPECT_EQ(copy3.getColumnSize(i), columns[i]);

    EXPECT_EQ(copy3.getNumEntries(), numOperations);

    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, sumTime);

    delete copy;
    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, insertIntoRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, insertToInner)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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


GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, singleBulkload)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, severalBulkloads)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, findPointInRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}), rReadTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5)), 5.0 * rReadTime);
        EXPECT_NEAR(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.1), (numOperations / 10) * rReadTime, 0.0001);
        EXPECT_NEAR(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.1, 2), 2.0 * (numOperations / 10) * rReadTime, 0.0001);

        sumTime += rReadTime + 5.0 * rReadTime + (numOperations / 10) * rReadTime + 2.0 * (numOperations / 10) * rReadTime;

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 +  (numOperations / 10) + 2 * (numOperations / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, findPointInBigTree)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double sWriteTime = 15.3 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;
    size_t numOperations = 10;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}), 2.0 * rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5)), 5.0 * (2.0 * rReadTime + rReadTime));
        EXPECT_NEAR(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.1), (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime), 0.001);
        EXPECT_NEAR(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.1, 2), 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime), 0.001);

        sumTime += 2.0 * rReadTime + rReadTime + 5.0 * (2.0 * rReadTime + rReadTime) + (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime) + 2.0 * (entriesToInsert / 10) * (2.0 * rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 5 + (entriesToInsert / 10) + 2 * (entriesToInsert / 10)));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, findRangeInRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5)), rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5), 2), 2.0 * (rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, nodeSize / recordSize), 2.0 * rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.1), rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.1, 2), 2.0 * (rReadTime + rReadTime));

        sumTime += rReadTime + rReadTime + 2.0 * (rReadTime + rReadTime) +  2.0 * rReadTime + rReadTime + rReadTime + 2.0 * (rReadTime + rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, findRangeInBigTree)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
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
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5)), 2.0 * rReadTime + rReadTime + rReadTime);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(5), 2), 2.0 * (2.0 * rReadTime + rReadTime + rReadTime));
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, nodeSize / recordSize), 2.0 * rReadTime + 2.0 * rReadTime);
        EXPECT_NEAR(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.1), 2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime, 0.0001);
        EXPECT_NEAR(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.1, 2), 2.0 * (2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime), 0.0001);

        sumTime += 2.0 * rReadTime + rReadTime + rReadTime + 2.0 * (2.0 * rReadTime + rReadTime + rReadTime) + 2.0 * rReadTime + 2.0 * rReadTime +  2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime + 2.0 * (2.0 * rReadTime + ((entriesToInsert / 10) / (nodeSize / recordSize) + 1) * rReadTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + 2 + 1 + 1 + 2));
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, deleteFromRoot)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize - 1;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnBPTreeSSDBasicTest, deleteFromInner)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numOperations = nodeSize / recordSize;
    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    const double eraseTime = 210.0 * 64.0 / 1000000.0;

    BPTree* bp = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* rawIndex = bp;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

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

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, insertIntoHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, insertIntoHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  45.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fd->getHeight(), 1);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
        EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  45.0 / 1000000.0 + pagesToWriteRound1 * 21.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, deleteFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 50;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), numOperations);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, deleteFromHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);
    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert without merge
    for (size_t i = 0; i < numOperations - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete without merge
    for (size_t i = 0 ; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  45.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations - 10);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations / 2 - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 1);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
        EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    for (size_t i = 0; i < numOperations / 2 + 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fd->getHeight(), 1);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), numOperations / 2 - 10);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
        EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // // merge happend here, LVL0 has 1 page
    const double expectedTimeRound2 = (pagesToWriteRound1) *  45.0 / 1000000.0 + pagesToWriteRound1 * 21.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - numOperations / 2 - 10 + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + numOperations / 2 - 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + numOperations / 2 + 10);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations - 10 - 20 + 1);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, mergeLevels)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  45.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fd->getHeight(), 1);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
        EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  45.0 / 1000000.0 + pagesToWriteRound1 * 21.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fd->getHeight(), 1);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // third round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

        EXPECT_EQ(fd->getHeight(), 1);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
        EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
        EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge 0 with 1, 1 with 2
    const size_t pagesToWriteRound3 = pagesToWriteRound1 * 2 + pagesToWriteRound2;
    const size_t pagesToReadRound3 = pagesToWriteRound2 + pagesToWriteRound1;
    const double expectedTimeRound3 = (pagesToWriteRound3) *  45.0 / 1000000.0 + pagesToReadRound3 * 21.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound3);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 3);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 3);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2 + expectedTimeRound3);


    EXPECT_EQ(fd->getHeight(), 2);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fd->getFDLvl(1).getNumEntries(), (numOperations + 1));
    EXPECT_EQ(fd->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(fd->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    EXPECT_EQ(fd->getFDLvl(2).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fd->getFDLvl(2).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(2).getLvl(), 2);
    EXPECT_EQ(fd->getFDLvl(2).getMaxEntries(), (headTreeSize * lvlRatio * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, bigInsertBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fd->getFDLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fd->getFDLvl(i).getNumEntries() + fd->getFDLvl(i).getNumEntriesToDelete(), fd->getFDLvl(i).getMaxEntries());
        EXPECT_EQ(fd->getFDLvl(i).getLvl(), i);
        EXPECT_EQ(fd->getFDLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, bigDeleteBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);
    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fd->getFDLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fd->getFDLvl(i).getNumEntries() + fd->getFDLvl(i).getNumEntriesToDelete(), fd->getFDLvl(i).getMaxEntries());
        EXPECT_EQ(fd->getFDLvl(i).getLvl(), i);
        EXPECT_EQ(fd->getFDLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    EXPECT_GT(index->deleteEntries(numOperations / 2), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations / 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, numOperations / 2);
    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl2[] = {0, 0, 0, 0, 0, 0, 0, 0, 14563, 57856};
    const size_t entriesToDeletePerLvl2[] = {46, 113, 452, 904, 904, 1808, 3616, 7232, 0, 0};
    size_t sumEntries = std::accumulate(entriesPerLvl2, entriesPerLvl2 + 10, 0);
    size_t sumEntriesToDelete =  std::accumulate(entriesToDeletePerLvl2, entriesToDeletePerLvl2 + 10, 0);

    ASSERT_EQ(sumEntries - sumEntriesToDelete,  numOperations / 2);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fd->getFDLvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(fd->getFDLvl(i).getNumEntries() + fd->getFDLvl(i).getNumEntriesToDelete(), fd->getFDLvl(i).getMaxEntries());
        EXPECT_EQ(fd->getFDLvl(i).getLvl(), i);
        EXPECT_EQ(fd->getFDLvl(i).getNumEntriesToDelete(), entriesToDeletePerLvl2[i]);
        EXPECT_EQ(fd->getFDLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, findPointFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.1 * numOperations)));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, findRangeFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fd->getHeight(), 0);

        EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
        EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(1)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 2);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, bigFindPointBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fd->getFDLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fd->getFDLvl(i).getNumEntries() + fd->getFDLvl(i).getNumEntriesToDelete(), fd->getFDLvl(i).getMaxEntries());
        EXPECT_EQ(fd->getFDLvl(i).getLvl(), i);
        EXPECT_EQ(fd->getFDLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}), 9 * (21.0 / 1000000.0));
        EXPECT_DOUBLE_EQ(index->findPointEntries(std::vector<size_t>{1, 2, 3}, 0.0001), static_cast<size_t>(0.0001 * numOperations) * 9 * (21.0 / 1000000.0));

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
        // to many C++ double arythmetic, a lot of double errors, instead check final time using wolfram
        // EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, (i + 1) * (9 * (21.0 / 1000000.0) + static_cast<size_t>(0.0001 * numOperations) * 9 * (21.0 / 1000000.0)));
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.22679999999999942);

    delete index;
}

GTEST_TEST(dbIndexRawToColumnFDTreeSSDBasicTest, bigFindRangeBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* rawIndex = fd;

    DBIndexColumn* index = new DBIndexRawToColumnWrapper(rawIndex, columns);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fd->getFDLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fd->getFDLvl(i).getNumEntries() + fd->getFDLvl(i).getNumEntriesToDelete(), fd->getFDLvl(i).getMaxEntries());
        EXPECT_EQ(fd->getFDLvl(i).getLvl(), i);
        EXPECT_EQ(fd->getFDLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fd->getFDLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(1)), 9 *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(20)), 9 *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, static_cast<size_t>(200)), (2 + 9) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.0001), ((static_cast<size_t>(0.0001 * numOperations) / recordSize) + 9) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(std::vector<size_t>{1, 2, 3}, 0.001), ((static_cast<size_t>(0.001 * numOperations) / recordSize) + 9) *  21.0 / 1000000.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 5);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.10079999999999958);

    delete index;
}