#include <index/fatree.hpp>
#include <disk/diskFlashNandFTL.hpp>
#include <disk/diskFlashNandRaw.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(fatreeBasicFlashFTLTest, interface)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("FlashNandFTL:micronMT29F32G08ABAAA"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 1048576);
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

    // specific for FA
    EXPECT_EQ(fa->getHeight(), 0);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, topology)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numEntries = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numEntries);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}


GTEST_TEST(fatreeBasicFlashFTLTest, insertIntoHeadTree)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, insertIntoHeadTreeWithMerge)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  350.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, copy)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  350.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree* copy = new FATree(*dynamic_cast<FATree*>(index));

    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree copy2(*copy);

    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy2.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy3.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, move)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  350.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree* copy = new FATree(std::move(*dynamic_cast<FATree*>(index)));

    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy->getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy->getHeight(), 1);

    EXPECT_EQ(copy->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree copy2(std::move(*copy));

    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy2.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy2.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy2.getHeight(), 1);

    EXPECT_EQ(copy2.getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    FATree copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getBlockSize(), 1048576);

    EXPECT_EQ(copy3.getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(copy3.getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(copy3.getHeight(), 1);

    EXPECT_EQ(copy3.getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getFALvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}


GTEST_TEST(fatreeBasicFlashFTLTest, deleteFromHeadTree)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 50;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, deleteFromHeadTreeWithMerge)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert without merge
    for (size_t i = 0; i < numOperations - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete without merge
    for (size_t i = 0 ; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = ((numOperations - 20) * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  350.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations / 2 - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    for (size_t i = 0; i < numOperations / 2 + 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations / 2 - 10);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // // merge happend here, LVL0 has 1 page
    const double expectedTimeRound2 = (pagesToWriteRound1) *  350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - numOperations / 2 - 10 + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + numOperations / 2 - 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + numOperations / 2 + 10);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10 - 20 + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, mergeLevels)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  350.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) *  350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // third round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge 0 with 1, 1 with 2
    const size_t pagesToWriteRound3 = pagesToWriteRound1 * 2 + pagesToWriteRound2;
    const size_t pagesToReadRound3 = pagesToWriteRound2 + pagesToWriteRound1;
    const double expectedTimeRound3 = (pagesToWriteRound3) *  350.0 / 1000000.0 + pagesToReadRound3 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound3);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 3);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 3);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2 + expectedTimeRound3);


    EXPECT_EQ(fa->getHeight(), 2);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1));
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    EXPECT_EQ(fa->getFALvl(2).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(2).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(2).getLvl(), 2);
    EXPECT_EQ(fa->getFALvl(2).getMaxEntries(), (headTreeSize * lvlRatio * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, bigInsertBatch)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, bigDeleteBatch)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);
    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    EXPECT_GT(index->deleteEntries(numOperations / 2), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations / 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, numOperations / 2);
    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl2[] = {0, 0, 0, 0, 0, 0, 0, 0, 14563, 57856};
    const size_t entriesToDeletePerLvl2[] = {46, 113, 452, 904, 904, 1808, 3616, 7232, 0, 0};
    size_t sumEntries = std::accumulate(entriesPerLvl2, entriesPerLvl2 + 10, 0);
    size_t sumEntriesToDelete =  std::accumulate(entriesToDeletePerLvl2, entriesToDeletePerLvl2 + 10, 0);

    ASSERT_EQ(sumEntries - sumEntriesToDelete,  numOperations / 2);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), entriesToDeletePerLvl2[i]);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, findPointFromHeadTree)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
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

GTEST_TEST(fatreeBasicFlashFTLTest, findRangeFromHeadTree)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
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


GTEST_TEST(fatreeBasicFlashFTLTest, bigFindPointBatch)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 9 * (35.0 / 1000000.0));
        // here we have some C++ arythmetic bug, the output (in Wolfram should be: 0.0034649999999999967)
        // EXPECT_DOUBLE_EQ(index->findPointEntries(0.0001), static_cast<size_t>(0.0001 * numOperations) * 9 * (35.0 / 1000000.0));
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.0001), 0.0034649999999999967);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
        // another double bug
        // EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, (i + 1) * 0.0034649999999999967);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.378000000000000222);

    delete index;
}

GTEST_TEST(fatreeBasicFlashFTLTest, bigFindRangeBatch)
{
    Disk* flash = new DiskFlashNandFTL_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(1)), 9 *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(20)), 9 *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(200)), (2 + 9) *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.0001), ((static_cast<size_t>(0.0001 * numOperations) * recordSize + nodeSize -1) / nodeSize + 8) *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.001), ((static_cast<size_t>(0.001 * numOperations) * recordSize + nodeSize -1) / nodeSize + 8) *  35.0 / 1000000.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 5);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.16800000000000012);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, interface)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    // check how to get some statistics
    EXPECT_EQ(std::string(index->getDisk().getLowLevelController().getModelName()), std::string("FlashNandRaw:micronMT29F32G08ABAAA"));
    EXPECT_EQ(index->getDisk().getLowLevelController().getPageSize(), 8192);
    EXPECT_EQ(index->getDisk().getLowLevelController().getBlockSize(), 1048576);
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

    // specific for FA
    EXPECT_EQ(fa->getHeight(), 0);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, insertIntoHeadTree)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, topology)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numEntries = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    index->createTopologyAfterInsert(numEntries);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numEntries);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, insertIntoHeadTreeWithMerge)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = 128 * (350.0 / 1000000.0);
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const double expectedTimeRound2 = 128 * (350.0 / 1000000.0) + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, deleteFromHeadTree)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 50;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, deleteFromHeadTreeWithMerge)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert without merge
    for (size_t i = 0; i < numOperations - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // delete without merge
    for (size_t i = 0 ; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = ((numOperations - 20) * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = 128 * (350.0 / 1000000.0);
    EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations / 2 - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    for (size_t i = 0; i < numOperations / 2 + 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), numOperations / 2 - 10);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // // merge happend here, LVL0 has 1 page
    const double expectedTimeRound2 = (128) * (350.0 / 1000000.0) + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - numOperations / 2 - 10 + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + numOperations / 2 - 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + numOperations / 2 + 10);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations - 10 - 20 + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, mergeLevels)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = (128) * (350.0 / 1000000.0);
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (flash->getLowLevelController().getPageSize() - 1)) / flash->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (128) * 350.0 / 1000000.0 + pagesToWriteRound1 * 35.0 / 1000000.0;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(fa->getHeight(), 1);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    // third round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

        EXPECT_EQ(fa->getHeight(), 1);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

        EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1) * 2);
        EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
        EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    }

    // merge 0 with 1, 1 with 2
    const size_t pagesToReadRound3 = pagesToWriteRound2 + pagesToWriteRound1;
    const double expectedTimeRound3 = (128 + 128) * (350.0 / 1000000.0) + pagesToReadRound3 * (35.0 / 1000000.0);
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound3);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 3);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 3);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2 + expectedTimeRound3);

    EXPECT_EQ(fa->getHeight(), 2);

    EXPECT_EQ(fa->getFALvl(0).getNumEntries(), 0);
    EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
    EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(fa->getFALvl(1).getNumEntries(), (numOperations + 1));
    EXPECT_EQ(fa->getFALvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(1).getLvl(), 1);
    EXPECT_EQ(fa->getFALvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    EXPECT_EQ(fa->getFALvl(2).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(fa->getFALvl(2).getNumEntriesToDelete(), 0);
    EXPECT_EQ(fa->getFALvl(2).getLvl(), 2);
    EXPECT_EQ(fa->getFALvl(2).getMaxEntries(), (headTreeSize * lvlRatio * lvlRatio) / recordSize);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, bigInsertBatch)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, bigDeleteBatch)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);
    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    EXPECT_GT(index->deleteEntries(numOperations / 2), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations / 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, numOperations / 2);
    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl2[] = {0, 0, 0, 0, 0, 0, 0, 0, 14563, 57856};
    const size_t entriesToDeletePerLvl2[] = {46, 113, 452, 904, 904, 1808, 3616, 7232, 0, 0};
    size_t sumEntries = std::accumulate(entriesPerLvl2, entriesPerLvl2 + 10, 0);
    size_t sumEntriesToDelete =  std::accumulate(entriesToDeletePerLvl2, entriesToDeletePerLvl2 + 10, 0);

    ASSERT_EQ(sumEntries - sumEntriesToDelete,  numOperations / 2);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), entriesToDeletePerLvl2[i]);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, findPointFromHeadTree)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
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

GTEST_TEST(fatreeBasicFlashRawTest, findRangeFromHeadTree)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 10;
    const size_t numOperations = 100;

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(fa->getHeight(), 0);

        EXPECT_EQ(fa->getFALvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(fa->getFALvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(0).getLvl(), 0);
        EXPECT_EQ(fa->getFALvl(0).getMaxEntries(), headTreeSize / recordSize);
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


GTEST_TEST(fatreeBasicFlashRawTest, bigFindPointBatch)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), 9 * (35.0 / 1000000.0));
        // here we have some C++ arythmetic bug, the output (in Wolfram should be: 0.0034649999999999967)
        // EXPECT_DOUBLE_EQ(index->findPointEntries(0.0001), static_cast<size_t>(0.0001 * numOperations) * 9 * (35.0 / 1000000.0));
        EXPECT_DOUBLE_EQ(index->findPointEntries(0.0001), 0.0034649999999999967);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
        // another double bug
        // EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, (i + 1) * 0.0034649999999999967);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.378000000000000222);

    delete index;
}

GTEST_TEST(fatreeBasicFlashRawTest, bigFindRangeBatch)
{
    Disk* flash = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = flash->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 10);

    FATree* fa = new FATree(flash, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = fa;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(fa->getHeight(), 9);

    const size_t entriesPerLvl[] = {106, 226, 452, 904, 904, 3616, 7232, 14464, 28928, 57856};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + 10, 0), numOperations);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(fa->getFALvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(fa->getFALvl(i).getNumEntries() + fa->getFALvl(i).getNumEntriesToDelete(), fa->getFALvl(i).getMaxEntries());
        EXPECT_EQ(fa->getFALvl(i).getLvl(), i);
        EXPECT_EQ(fa->getFALvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(fa->getFALvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(1)), 9 *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(20)), 9 *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(200)), (2 + 9) *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.0001), ((static_cast<size_t>(0.0001 * numOperations) * recordSize + nodeSize -1) / nodeSize + 8) *  35.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.001), ((static_cast<size_t>(0.001 * numOperations) * recordSize + nodeSize -1) / nodeSize + 8) *  35.0 / 1000000.0);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 5);
    }

    // calculated in wolfram to avoiid C++ double errors
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.16800000000000012);

    delete index;
}