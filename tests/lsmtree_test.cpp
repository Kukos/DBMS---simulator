#include <index/lsmtree.hpp>
#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>
#include <numeric>

#include <gtest/gtest.h>

GTEST_TEST(lsmtreeBasicTest, interface)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

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

    // specific for LSM
    EXPECT_EQ(lsm->getHeight(), 0);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, insertIntoBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, insertIntoBufferTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;


    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 * writeTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);


    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) * writeTime + pagesToWriteRound1 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, copy)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;


    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 * writeTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);


    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) * writeTime + pagesToWriteRound1 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    LSMTree* copy = new LSMTree(*dynamic_cast<LSMTree*>(index));

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

    EXPECT_EQ(copy->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    LSMTree copy2(*copy);

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

    EXPECT_EQ(copy2.getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    LSMTree copy3;
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

    EXPECT_EQ(copy3.getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}

GTEST_TEST(lsmtreeBasicTest, move)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;


    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 * writeTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);


    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) * writeTime + pagesToWriteRound1 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    LSMTree* copy = new LSMTree(std::move(*dynamic_cast<LSMTree*>(index)));

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

    EXPECT_EQ(copy->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    LSMTree copy2(std::move(*copy));

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

    EXPECT_EQ(copy2.getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy2.getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy2.getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy2.getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy2.getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy2.getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    LSMTree copy3;
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

    EXPECT_EQ(copy3.getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(copy3.getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);

    EXPECT_EQ(copy3.getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(copy3.getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(copy3.getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(copy3.getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);

    delete copy;
    delete index;
}

GTEST_TEST(lsmtreeBasicTest, deleteFromBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert
    for (size_t i = 0; i < numOperations / 2; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // delete
    for (size_t i = 0; i < numOperations / 4; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations / 2 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numOperations / 2);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, deleteFromBufferTreeWithMerge)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // insert without merge
    for (size_t i = 0; i < numOperations - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // delete without merge
    for (size_t i = 0 ; i < 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numOperations - 10);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = ((numOperations - 20) * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 * writeTime;
    EXPECT_DOUBLE_EQ(index->deleteEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);


    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations - 10);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations / 2 - 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    for (size_t i = 0; i < numOperations / 2 + 10; ++i)
    {
        EXPECT_DOUBLE_EQ(index->deleteEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - i - 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numOperations / 2 - 10);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations - 10);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 11);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // // merge happend here, LVL0 has 1 page
    const double expectedTimeRound2 = (pagesToWriteRound1) * writeTime + pagesToWriteRound1 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), numOperations - 20 - 1 + numOperations / 2 - 10 - numOperations / 2 - 10 + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations - 10 + numOperations / 2 - 10 + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound2);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 10 + 1 + numOperations / 2 + 10);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations - 10 - 20 + 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 11);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, mergeLevels)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 2;
    const size_t numOperations = (headTreeSize / recordSize) - 1;
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    // merge happend here, LVL0 is empty so only writing occured
    const size_t pagesToWriteRound1 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound1 = pagesToWriteRound1 *  writeTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound1);
    EXPECT_EQ(index->getNumEntries(), numOperations + 1);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    // second round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), numOperations + 1 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations + 1 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), numOperations + 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // merge happend here, LVL0 has 1 page
    const size_t pagesToWriteRound2 = (numOperations * recordSize + (ssd->getLowLevelController().getPageSize() - 1)) / ssd->getLowLevelController().getPageSize();
    const double expectedTimeRound2 = (pagesToWriteRound2 + pagesToWriteRound1) * writeTime + pagesToWriteRound1 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound2);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

    EXPECT_EQ(lsm->getHeight(), 1);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    // third round
    // without merge
    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 2 + i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 2 + i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2);

        EXPECT_EQ(lsm->getHeight(), 1);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1) * 2);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
        EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);
    }

    // merge 0 with 1, 1 with 2
    const size_t pagesToWriteRound3 = pagesToWriteRound1 * 2 + pagesToWriteRound2;
    const size_t pagesToReadRound3 = pagesToWriteRound2 + pagesToWriteRound1;
    const double expectedTimeRound3 = (pagesToWriteRound3) *  writeTime + pagesToReadRound3 * readTime;
    EXPECT_DOUBLE_EQ(index->insertEntries(), expectedTimeRound3);
    EXPECT_EQ(index->getNumEntries(), (numOperations + 1) * 3);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, (numOperations + 1) * 3);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, expectedTimeRound1 + expectedTimeRound2 + expectedTimeRound3);


    EXPECT_EQ(lsm->getHeight(), 2);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntries(), (numOperations + 1));
    EXPECT_EQ(lsm->getLSMLvl(1).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(1).getLvl(), 1);
    EXPECT_EQ(lsm->getLSMLvl(1).getMaxEntries(), (headTreeSize * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(1).getNumOfNodes(), 1 * lvlRatio);

    EXPECT_EQ(lsm->getLSMLvl(2).getNumEntries(), (numOperations + 1) * 2);
    EXPECT_EQ(lsm->getLSMLvl(2).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(2).getLvl(), 2);
    EXPECT_EQ(lsm->getLSMLvl(2).getMaxEntries(), (headTreeSize * lvlRatio * lvlRatio) / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(2).getNumOfNodes(), 1 * lvlRatio * lvlRatio);

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bigInsertBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bigDeleteBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    EXPECT_GT(index->deleteEntries(numOperations / 2), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations / 2);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, numOperations / 2);
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl2[] = {0, 0, 0, 0, 0, 2132447};
    const size_t entriesToDeletePerLvl2[] = {429, 4548, 11370, 56850, 284250, 0};
    size_t sumEntries = std::accumulate(entriesPerLvl2, entriesPerLvl2 + expectedHeight + 1, 0);
    size_t sumEntriesToDelete =  std::accumulate(entriesToDeletePerLvl2, entriesToDeletePerLvl2 + expectedHeight + 1, 0);

    ASSERT_EQ(sumEntries - sumEntriesToDelete,  numOperations / 2);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), entriesToDeletePerLvl2[i]);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, findPointFromBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
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

GTEST_TEST(lsmtreeBasicTest, findRangeFromBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations; ++i)
    {
        EXPECT_DOUBLE_EQ(index->insertEntries(), 0.0);

        EXPECT_EQ(index->getNumEntries(), i + 1);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, i + 1);
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), i + 1);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
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

GTEST_TEST(lsmtreeBasicTest, bigFindPointBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    double sumTime = 0.0;
    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findPointEntries(), expectedHeight * 4 * (21.0 / 1000000.0));
        EXPECT_NEAR(index->findPointEntries(0.0001), static_cast<size_t>(0.0001 * numOperations) * expectedHeight * 4 * (21.0 / 1000000.0), 0.0001);

        sumTime += expectedHeight * 4 * (21.0 / 1000000.0) + static_cast<size_t>(0.0001 * numOperations) * expectedHeight * 4 * (21.0 / 1000000.0);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, sumTime, 0.001);

        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, (i + 1) * (1 + static_cast<size_t>(0.0001 * numOperations)));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bigFindRangeBatch)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    double sumTime = 0.0;
    for (size_t i = 0; i < 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(1)), expectedHeight * 4 * 21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(20)), (expectedHeight * 4 + 1) * 21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(static_cast<size_t>(200)), (2 + expectedHeight * 4) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.0001), ((static_cast<size_t>(0.0001 * numOperations) * recordSize + pageSize -1) / pageSize + 2) * 14.0 / 1000000.0  + (expectedHeight * 4 - 2) *  21.0 / 1000000.0);
        EXPECT_DOUBLE_EQ(index->findRangeEntries(0.001), ((static_cast<size_t>(0.001 * numOperations) * recordSize + pageSize - 1) / pageSize + 1) * 14.0 / 1000000.0 + (expectedHeight * 4 - 2) *  21.0 / 1000000.0);

        sumTime += (expectedHeight * 4 * 21.0 / 1000000.0) +
                   (expectedHeight * 4 + 1) * 21.0 / 1000000.0 +
                   (2 + expectedHeight * 4) *  21.0 / 1000000.0 +
                   ((static_cast<size_t>(0.0001 * numOperations) * recordSize + pageSize -1) / pageSize + 2) * 14.0 / 1000000.0  + (expectedHeight * 4 - 2) *  21.0 / 1000000.0 +
                   ((static_cast<size_t>(0.001 * numOperations) * recordSize + pageSize - 1) / pageSize + 1) * 14.0 / 1000000.0 + (expectedHeight * 4 - 2) *  21.0 / 1000000.0;


        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, (i + 1) * 5);
        EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, sumTime, 0.001);
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadCurrIntoBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations / 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->bulkloadEntries(100), 0.0);

        EXPECT_EQ(index->getNumEntries(), (i + 1) * 100);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), (i + 1) * 100);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadCurrIntoEmptyTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const double writeTime = 15.3 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY);
    DBIndex* index = lsm;

    size_t numEntries = (headTreeSize / recordSize) - 1;
    ASSERT_LE(recordSize * numEntries, headTreeSize);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(numEntries), 0.0);
    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(lsm->getHeight(), 0);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numEntries);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    const size_t entriesPerNode = (nodeSize + recordSize);
    EXPECT_DOUBLE_EQ(index->bulkloadEntries(entriesPerNode * 100), ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    const size_t expectedHeight = 6;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries[] = {1136, 5687, 28443, 142221, 711110, 3555554, 3756185};

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, mergeAfterBulkloadCurr)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const double writeTime = 15.3 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY);
    DBIndex* index = lsm;

    size_t numEntries = (headTreeSize / recordSize) - 1;
    ASSERT_LE(recordSize * numEntries, headTreeSize);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(numEntries), 0.0);
    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(lsm->getHeight(), 0);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numEntries);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    const size_t entriesPerNode = (nodeSize + recordSize);
    numEntries += entriesPerNode * 100;
    EXPECT_DOUBLE_EQ(index->bulkloadEntries(entriesPerNode * 100), ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    const size_t expectedHeight = 6;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries[] = {1136, 5687, 28443, 142221, 711110, 3555554, 3756185};
    ASSERT_EQ(std::accumulate(expectedEntries, expectedEntries + expectedHeight + 1, 0), numEntries);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    EXPECT_GT(index->insertEntries(), 0.0);
    ++numEntries;

    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries2[] = {0, 1137, 5687, 28443, 142221, 711110, 7311739};
    ASSERT_EQ(std::accumulate(expectedEntries2, expectedEntries2 + expectedHeight + 1, 0), numEntries);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadCurrIntoLevel)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(2000), ((2274 * recordSize + pageSize - 1) / pageSize) * readTime + ((4274 * recordSize + pageSize - 1) / pageSize) * writeTime);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, ((2274 * recordSize + pageSize - 1) / pageSize) * readTime + ((4274 * recordSize + pageSize - 1) / pageSize) * writeTime);

    EXPECT_EQ(index->getNumEntries(), numOperations + 2000);

    const size_t entriesPerLvl2[] = {286, 4274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl2, entriesPerLvl2 + expectedHeight + 1, 0), numOperations + 2000);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}


GTEST_TEST(lsmtreeBasicTest, bulkloadMaxIntoBufferTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = (headTreeSize / recordSize) - 1;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);
    DBIndex* index = lsm;

    ASSERT_LE(recordSize * numOperations, headTreeSize);

    for (size_t i = 0; i < numOperations / 100; ++i)
    {
        EXPECT_DOUBLE_EQ(index->bulkloadEntries(100), 0.0);

        EXPECT_EQ(index->getNumEntries(), (i + 1) * 100);
        EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, (i + 1));
        EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

        EXPECT_EQ(lsm->getHeight(), 0);

        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), (i + 1) * 100);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
        EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadMaxIntoEmptyTree)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const double writeTime = 15.3 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);
    DBIndex* index = lsm;

    size_t numEntries = (headTreeSize / recordSize) - 1;
    ASSERT_LE(recordSize * numEntries, headTreeSize);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(numEntries), 0.0);
    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(lsm->getHeight(), 0);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numEntries);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    const size_t entriesPerNode = (nodeSize + recordSize);
    EXPECT_DOUBLE_EQ(index->bulkloadEntries(entriesPerNode * 100), ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    const size_t expectedHeight = 6;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries[] = {1136, 5687, 28443, 142221, 711110, 3555554, 3756185};

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, mergeAfterBulkloadMax)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const double writeTime = 15.3 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);
    DBIndex* index = lsm;

    size_t numEntries = (headTreeSize / recordSize) - 1;
    ASSERT_LE(recordSize * numEntries, headTreeSize);

    EXPECT_DOUBLE_EQ(index->bulkloadEntries(numEntries), 0.0);
    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(lsm->getHeight(), 0);

    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntries(), numEntries);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumEntriesToDelete(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getLvl(), 0);
    EXPECT_EQ(lsm->getLSMLvl(0).getMaxEntries(), headTreeSize / recordSize);
    EXPECT_EQ(lsm->getLSMLvl(0).getNumOfNodes(), 1);

    const size_t entriesPerNode = (nodeSize + recordSize);
    numEntries += entriesPerNode * 100;
    EXPECT_DOUBLE_EQ(index->bulkloadEntries(entriesPerNode * 100), ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    EXPECT_EQ(index->getNumEntries(), numEntries);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, ((entriesPerNode * 100 * recordSize) / pageSize + 1) * writeTime);

    const size_t expectedHeight = 6;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries[] = {1136, 5687, 28443, 142221, 711110, 3555554, 3756185};
    ASSERT_EQ(std::accumulate(expectedEntries, expectedEntries + expectedHeight + 1, 0), numEntries);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    EXPECT_GT(index->insertEntries(), 0.0);
    ++numEntries;

    EXPECT_EQ(lsm->getHeight(), expectedHeight);
    const size_t expectedEntries2[] = {0, 1137, 5687, 28443, 142221, 711110, 7311739};
    ASSERT_EQ(std::accumulate(expectedEntries2, expectedEntries2 + expectedHeight + 1, 0), numEntries);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), expectedEntries2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadMaxIntoLevel)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    const size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    const double expectedBulkloadTime = ((2274 * recordSize + pageSize - 1) / pageSize) * readTime + ((22740 * recordSize + pageSize - 1) / pageSize) * readTime + ((2274 + 22740 * recordSize + pageSize - 1) / pageSize) * writeTime + ((4000 * recordSize + pageSize - 1) / pageSize) * writeTime;
    EXPECT_NEAR(index->bulkloadEntries(4000), expectedBulkloadTime, 0.001);

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_NEAR(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedBulkloadTime, 0.001);

    EXPECT_EQ(index->getNumEntries(), numOperations + 4000);

    const size_t entriesPerLvl2[] = {286, 4000, 25014, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl2, entriesPerLvl2 + expectedHeight + 1, 0), numOperations + 4000);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}

GTEST_TEST(lsmtreeBasicTest, bulkloadMaxIntoLowestLevel)
{
    Disk* ssd = new DiskSSD_Samsung840();

    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = ssd->getLowLevelController().getPageSize() * 10;
    const size_t pageSize = ssd->getLowLevelController().getPageSize();
    const size_t headTreeSize = nodeSize;
    const size_t lvlRatio = 5;
    const size_t numOperations = ((headTreeSize / recordSize) - 1) * std::pow(lvlRatio, 5);
    const double writeTime = 15.3 / 1000000.0;
    const double readTime = 14.0 / 1000000.0;

    LSMTree* lsm = new LSMTree(ssd, keySize, dataSize, nodeSize, headTreeSize, lvlRatio, LSMTree::BULKLOAD_ACCORDING_TO_MAX_CAPACITY);
    DBIndex* index = lsm;

    EXPECT_GT(index->insertEntries(numOperations), 0.0);

    EXPECT_EQ(index->getNumEntries(), numOperations);
    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    size_t expectedHeight = 5;
    EXPECT_EQ(lsm->getHeight(), expectedHeight);

    const size_t entriesPerLvl[] = {286, 2274, 22740, 113700, 568500, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl, entriesPerLvl + expectedHeight + 1, 0), numOperations);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    const size_t entriesToBulkload = lsm->getLSMLvl(expectedHeight).getMaxEntries() - 1;
    const double expectedBulkloadTime = ((2842500 * recordSize + pageSize - 1) / pageSize) * readTime + ((2842500 * recordSize + pageSize - 1) / pageSize) * writeTime + ((entriesToBulkload * recordSize + pageSize - 1) / pageSize) * writeTime;
    EXPECT_DOUBLE_EQ(index->bulkloadEntries(entriesToBulkload), expectedBulkloadTime);
    ++expectedHeight;

    EXPECT_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, expectedBulkloadTime);

    EXPECT_EQ(index->getNumEntries(), numOperations + entriesToBulkload);

    const size_t entriesPerLvl2[] = {286, 2274, 22740, 113700, 568500, entriesToBulkload, 2842500};
    ASSERT_EQ(std::accumulate(entriesPerLvl2, entriesPerLvl2 + expectedHeight + 1, 0), numOperations + entriesToBulkload);

    for (size_t i = 0; i <= expectedHeight; ++i)
    {
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntries(), entriesPerLvl2[i]);
        EXPECT_LT(lsm->getLSMLvl(i).getNumEntries() + lsm->getLSMLvl(i).getNumEntriesToDelete(), lsm->getLSMLvl(i).getMaxEntries());
        EXPECT_EQ(lsm->getLSMLvl(i).getLvl(), i);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumEntriesToDelete(), 0);
        EXPECT_EQ(lsm->getLSMLvl(i).getMaxEntries(), (headTreeSize * static_cast<size_t>(std::floor(std::pow(lvlRatio, i)))) / recordSize);
        EXPECT_EQ(lsm->getLSMLvl(i).getNumOfNodes(), std::pow(lvlRatio, i));
    }

    delete index;
}