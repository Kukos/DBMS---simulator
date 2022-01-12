#include <index/cfdtree.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(cfdtreeBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

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

    // specific for CFD
    EXPECT_EQ(cfd->getHeight(), 0);

    const size_t maxEntries = nodeSize / keySize;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        EXPECT_EQ(cfd->getCFDLvl(0, i).getNumEntries(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, i).getLvl(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, i).getMaxEntries(), maxEntries);
    }

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, topology)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numEntries = ((headTreeSize / keySize) - 1) * std::pow(lvlRatio, 4);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    size_t expectedHeight = 4;
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {240, 9216, 82944, 1013760, 9123840};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numEntries);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }


    delete index;
}

GTEST_TEST(cfdtreeBasicTest, insertIntoHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = (headTreeSize / keySize) - 1;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    delete index;
}


GTEST_TEST(cfdtreeBasicTest, insertIntoHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = (headTreeSize / keySize) - 1;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    // merge happend here, LVL0 is empty so only writing occured
    size_t pagesToWriteRound1 = 0;
    double expectedTimeRound1 = 0.0;
    double expectedTimeReadRound2 = 0.0;
    for (size_t i = 0; i < numColumns; ++i)
    {
        const size_t pagesToWriteRound1Temp = (numOperations * (keySize + (i == 0 ? 0 : columns[i])) + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
        expectedTimeRound1 += (pagesToWriteRound1Temp >= 4 ? 15.3 / 1000000.0 : 45.0 / 1000000.0) * pagesToWriteRound1Temp;
        expectedTimeReadRound2 += (pagesToWriteRound1Temp >= 4 ? 14.0 / 1000000.0 : 21.0 / 1000000.0) * pagesToWriteRound1Temp;

        pagesToWriteRound1 += pagesToWriteRound1Temp;
    }

    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(cfd->getHeight(), 1);

    for (size_t j = 0; j < numColumns; ++j)
    {
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);

        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntries(), numOperations + 1);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getLvl(), 1);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getMaxEntries(), (headTreeSize * lvlRatio) / keySize);
    }

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(cfd->getHeight(), 1);

        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);

            EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntries(), numOperations + 1);
            EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(1, j).getLvl(), 1);
            EXPECT_EQ(cfd->getCFDLvl(1, j).getMaxEntries(), (headTreeSize * lvlRatio) / keySize);
        }
    }

    // merge happend here, LVL0 has 1 page
    size_t pagesToWriteRound2 = 0;
    double expectedTimeRound2 = expectedTimeReadRound2;
    for (size_t i = 0; i < numColumns; ++i)
    {
        const size_t pagesToWriteRound2Temp = ((2 * numOperations) * (keySize + (i == 0 ? 0 : columns[i])) + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
        expectedTimeRound2 += (pagesToWriteRound2Temp >= 4 ? 15.3 / 1000000.0 : 45.0 / 1000000.0) * pagesToWriteRound2Temp;

        pagesToWriteRound2 += pagesToWriteRound2Temp;
    }

    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, ((numOperations + 1) * 2));
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(cfd->getHeight(), 1);

    for (size_t j = 0; j < numColumns; ++j)
    {
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);

        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntries(), (numOperations + 1) * 2);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getLvl(), 1);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getMaxEntries(), (headTreeSize * lvlRatio) / keySize);
    }

    delete index;
}


GTEST_TEST(cfdtreeBasicTest, deleteFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1) / 4;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    // delete
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), numOperations);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, deleteFromHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    // insert without merge
    for (size_t i = 0; i < numOperations - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    // delete without merge
    for (size_t i = 0 ; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), numOperations - 10);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    // merge happend here, LVL0 has 1 page
    size_t pagesToWriteRound1 = 0;
    double expectedTimeRound1 = 0.0;
    for (size_t i = 0; i < numColumns; ++i)
    {
        const size_t pagesToWriteRound1Temp = ((numOperations - 20) * (keySize + (i == 0 ? 0 : columns[i])) + ssd->getLowLevelController().getPageSize() - 1) / ssd->getLowLevelController().getPageSize();
        expectedTimeRound1 += (pagesToWriteRound1Temp >= 4 ? 15.3 / 1000000.0 : 45.0 / 1000000.0) * pagesToWriteRound1Temp;

        pagesToWriteRound1 += pagesToWriteRound1Temp;
    }
    EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(cfd->getHeight(), 1);

    for (size_t j = 0; j < numColumns; ++j)
    {
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
        EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);

        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntries(), numOperations - 10);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getNumEntriesToDelete(), 11);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getLvl(), 1);
        EXPECT_EQ(cfd->getCFDLvl(1, j).getMaxEntries(), (headTreeSize * lvlRatio) / keySize);
    }

    delete index;
}


GTEST_TEST(cfdtreeBasicTest, bigInsertBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1) * std::pow(lvlRatio, 4);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    size_t expectedHeight = 4;
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {240, 9216, 82944, 1013760, 9123840};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }


    delete index;
}

GTEST_TEST(cfdtreeBasicTest, bigDeleteBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1) * std::pow(lvlRatio, 4);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    size_t expectedHeight = 4;
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {240, 9216, 82944, 1013760, 9123840};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }

    EXPECT_GT(index->deleteEntries(numOperations / 2), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations / 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, numOperations / 2);
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl2[] = {0, 0, 0, 0, 6082016};
    const size_t entriesToDeletePerLvl2[] = {360, 8192, 46080, 912384, 0};
    size_t sumEntries = std::accumulate(entriesPerLvl2, entriesPerLvl2 + expectedHeight + 1, 0);
    size_t sumEntriesToDelete =  std::accumulate(entriesToDeletePerLvl2, entriesToDeletePerLvl2 + expectedHeight + 1, 0);

    ASSERT_EQ(sumEntries - sumEntriesToDelete,  numOperations / 2);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl2[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), entriesToDeletePerLvl2[i]);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, findPointFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = (headTreeSize / keySize) - 1;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    const std::vector<size_t> columnsToFetch {3, 0, 2};
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(columnsToFetch), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(columnsToFetch, 0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.1 * numOperations)));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, findRangeFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = (headTreeSize / keySize) - 1;

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    ASSERT_LE(keySize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(cfd->getHeight(), 0);
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntries(), i + 1);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getLvl(), 0);
            EXPECT_EQ(cfd->getCFDLvl(0, j).getMaxEntries(), headTreeSize / keySize);
        }
    }

    const std::vector<size_t> columnsToFetch {3, 0, 2};
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, static_cast<size_t>(1)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, 0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 2);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, bigFindPointBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1) * std::pow(lvlRatio, 4);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    size_t expectedHeight = 4;
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {240, 9216, 82944, 1013760, 9123840};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }


    const std::vector<size_t> columnsToFetch {3, 0, 2};
    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_NEAR(index->findPointEntries(columnsToFetch), (expectedHeight * columnsToFetch.size()) * (21.0 / 1000000.0), 0.00001);
        EXPECT_NEAR(index->findPointEntries(columnsToFetch, 0.0001), static_cast<size_t>(0.0001 * numOperations) * (expectedHeight * columnsToFetch.size()) * (21.0 / 1000000.0), 0.00001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
    }

    // calculated in wolfram to avoid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 25.804799999997861);

    delete index;
}

GTEST_TEST(cfdtreeBasicTest, bigFindRangeBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 16 + 32 + 4 + 4 + 8;
    const std::vector<size_t> columns = {8, 16, 32, 4, 4, 8};
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numColumns = columns.size();
    const size_t numOperations = ((headTreeSize / keySize) - 1) * std::pow(lvlRatio, 4);

    CFDTree* cfd = new CFDTree(ssd, columns, nodeSize, headTreeSize, lvlRatio);
    DBIndexColumn* index = cfd;

    EXPECT_EQ(index->getNumEntries(), 0);
    EXPECT_EQ(index->getKeySize(), keySize);
    EXPECT_EQ(index->getDataSize(), dataSize);
    EXPECT_EQ(index->getRecordSize(), recordSize);
    EXPECT_EQ(index->isBulkloadSupported(), false);

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    size_t expectedHeight = 4;
    EXPECT_EQ(cfd->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {240, 9216, 82944, 1013760, 9123840};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i < expectedHeight + 1; ++i)
        for (size_t j = 0; j < numColumns; ++j)
        {
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntries(), entriesPerLvl[i]);
            EXPECT_LT(cfd->getCFDLvl(i, j).getNumEntries() + cfd->getCFDLvl(i, j).getNumEntriesToDelete(), cfd->getCFDLvl(i, j).getMaxEntries());
            EXPECT_EQ(cfd->getCFDLvl(i, j).getLvl(), i);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getNumEntriesToDelete(), 0);
            EXPECT_EQ(cfd->getCFDLvl(i, j).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / keySize);
        }


    const std::vector<size_t> columnsToFetch {3, 0, 2};
    for (size_t i = 0; i < 1; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, static_cast<size_t>(1)), (expectedHeight * columnsToFetch.size()) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, static_cast<size_t>(20)), (expectedHeight * columnsToFetch.size()) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, static_cast<size_t>(200)), (1 + expectedHeight * columnsToFetch.size()) *  21.0 / 1000000.0);

        double expectedTime = 0.0;
        for (size_t j = 0; j < columnsToFetch.size(); ++j)
        {
            const size_t pagesToLoad = ((static_cast<size_t>(0.001 * numOperations) * (keySize + (columnsToFetch[j] == 0 ? 0.0 : columns[columnsToFetch[j]])) + nodeSize - 1) / nodeSize);
            expectedTime += (expectedHeight) * 21.0 / 1000000.0 + (pagesToLoad >= 4 ? 14.0 / 1000000.0 : 21.0 / 1000000.0) * pagesToLoad;
        }
        EXPECT_DOUBLE_EQ(index->findRangeEntries(columnsToFetch, 0.001), expectedTime);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 4);
    }

    // calculated in wolfram to avoid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0020790000000000001);

    delete index;
}