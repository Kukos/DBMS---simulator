#include <index/fdtree.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(fdtreeBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

    // specific for FD
    EXPECT_EQ(fd->getHeight(), 0);

    EXPECT_EQ(fd->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(fd->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    delete index;
}

GTEST_TEST(fdtreeBasicTest, topology)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numEntries = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(fd->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numEntries);

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

GTEST_TEST(fdtreeBasicTest, insertIntoHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, insertIntoHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, copy)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

    FDTree* copy = new FDTree(*dynamic_cast<FDTree*>(index));

    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy->getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FDTree copy2(*copy);

    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy2.getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy2.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FDTree copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy3.getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy3.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}

GTEST_TEST(fdtreeBasicTest, move)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

    FDTree* copy = new FDTree(std::move(*dynamic_cast<FDTree*>(index)));

    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy->getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FDTree copy2(std::move(*copy));

    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy2.getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy2.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FDTree copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(std::string(copy3.getDisk().getLowLevelController().getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getBlockSize(), 524288);

    EXPECT_EQ(copy3.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getFDLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getFDLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getFDLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getFDLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFDLvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getFDLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}

GTEST_TEST(fdtreeBasicTest, deleteFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 50;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, deleteFromHeadTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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
    const size_t pagesToWriteRound1 = ((numOperations - 20) * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
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

GTEST_TEST(fdtreeBasicTest, mergeLevels)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, bigInsertBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, bigDeleteBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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

GTEST_TEST(fdtreeBasicTest, findPointFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 0.0);
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.1 * numOperations)));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(fdtreeBasicTest, findRangeFromHeadTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(1)), 0.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.1), 0.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 2);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    }

    delete index;
}

GTEST_TEST(fdtreeBasicTest, bigFindPointBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 9 * (21.0 / 1000000.0));
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.0001), static_cast<size_t>(0.0001 * numOperations) * 9 * (21.0 / 1000000.0));

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
        // to many C++ double arythmetic, a lot of double errors, instead check final time using wolfram
        // EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, (i + 1) * (9 * (21.0 / 1000000.0) + static_cast<size_t>(0.0001 * numOperations) * 9 * (21.0 / 1000000.0)));
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.22679999999999942);

    delete index;
}

GTEST_TEST(fdtreeBasicTest, bigFindRangeBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FDTree* fd = new FDTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fd;

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
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(1)), 9 *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(20)), 9 *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(200)), (2 + 9) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.0001), ((static_cast<size_t>(0.0001 * numOperations) * recordSize + nodeSize -1) / nodeSize + 8) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.001), ((static_cast<size_t>(0.001 * numOperations) * recordSize + nodeSize - 1) / nodeSize + 8) *  21.0 / 1000000.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 5);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.10079999999999958);

    delete index;
}