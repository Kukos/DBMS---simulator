#include <adaptiveMerging/lazyAdaptiveMerging.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <index/bptree.hpp>

#include <gtest/gtest.h>

GTEST_TEST(lazyAdaptiveMerging, interface)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;

    const size_t reorganizationBlocksTreshold = 100;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

    EXPECT_EQ(manager.getNumEntries(), startingEntries);
    EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    DBIndex* copy = am->clone();

    delete copy;
    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, copy)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;

    const size_t reorganizationBlocksTreshold = 100;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

    EXPECT_EQ(manager.getNumEntries(), startingEntries);
    EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging* copy = new LazyAdaptiveMerging(*dynamic_cast<LazyAdaptiveMerging*>(am));

    EXPECT_EQ(std::string(copy->getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getCounter(id).second, 0L);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy->getNumEntries(), startingEntries);
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager2 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy->getMemoryManager());

    EXPECT_EQ(manager2.getNumEntries(), startingEntries);
    EXPECT_EQ(manager2.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager2.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager2.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager2.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager2.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging copy2(*copy);

    EXPECT_EQ(std::string(copy2.getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getCounter(id).second, 0L);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.getNumEntries(), startingEntries);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager3 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy2.getMemoryManager());

    EXPECT_EQ(manager3.getNumEntries(), startingEntries);
    EXPECT_EQ(manager3.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager3.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager3.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager3.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager3.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging copy3;
    copy3 = copy2;

    EXPECT_EQ(std::string(copy3.getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getCounter(id).second, 0L);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy3.getNumEntries(), startingEntries);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager4 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy3.getMemoryManager());

    EXPECT_EQ(manager4.getNumEntries(), startingEntries);
    EXPECT_EQ(manager4.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager4.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager4.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager4.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager4.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    delete am;
    delete copy;
}

GTEST_TEST(lazyAdaptiveMerging, move)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;

    const size_t reorganizationBlocksTreshold = 100;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(am->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(am->getCounter(id).second, 0L);

    EXPECT_EQ(am->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(am->getKeySize(), keySize);
    EXPECT_EQ(am->getDataSize(), dataSize);
    EXPECT_EQ(am->getRecordSize(), recordSize);
    EXPECT_EQ(am->isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

    EXPECT_EQ(manager.getNumEntries(), startingEntries);
    EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging* copy = new LazyAdaptiveMerging(std::move(*dynamic_cast<LazyAdaptiveMerging*>(am)));

    EXPECT_EQ(std::string(copy->getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy->getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy->getCounter(id).second, 0L);

    EXPECT_EQ(copy->getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy->getNumEntries(), startingEntries);
    EXPECT_EQ(copy->getKeySize(), keySize);
    EXPECT_EQ(copy->getDataSize(), dataSize);
    EXPECT_EQ(copy->getRecordSize(), recordSize);
    EXPECT_EQ(copy->isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager2 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy->getMemoryManager());

    EXPECT_EQ(manager2.getNumEntries(), startingEntries);
    EXPECT_EQ(manager2.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager2.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager2.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager2.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager2.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging copy2(std::move(*copy));

    EXPECT_EQ(std::string(copy2.getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy2.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy2.getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy2.getCounter(id).second, 0L);

    EXPECT_EQ(copy2.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.getNumEntries(), startingEntries);
    EXPECT_EQ(copy2.getKeySize(), keySize);
    EXPECT_EQ(copy2.getDataSize(), dataSize);
    EXPECT_EQ(copy2.getRecordSize(), recordSize);
    EXPECT_EQ(copy2.isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager3 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy2.getMemoryManager());

    EXPECT_EQ(manager3.getNumEntries(), startingEntries);
    EXPECT_EQ(manager3.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager3.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager3.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager3.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager3.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    LazyAdaptiveMerging copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(std::string(copy3.getName()), std::string("LazyAdaptiveMerging"));
    EXPECT_EQ(copy3.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getDisk().getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getDisk().getDiskCounter(id).second, 0L);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getDisk().getLowLevelController().getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getDisk().getLowLevelController().getCounter(id).second, 0L);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; id < IndexCounters::INDEX_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(copy3.getCounter(id).second, 0.0);

    for (auto id = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < IndexCounters::INDEX_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(copy3.getCounter(id).second, 0L);

    EXPECT_EQ(copy3.getDisk().getLowLevelController().getMemoryWearOut(), 0);

    EXPECT_EQ(copy3.getNumEntries(), startingEntries);
    EXPECT_EQ(copy3.getKeySize(), keySize);
    EXPECT_EQ(copy3.getDataSize(), dataSize);
    EXPECT_EQ(copy3.getRecordSize(), recordSize);
    EXPECT_EQ(copy3.isBulkloadSupported(), false);

    const LazyAdaptiveMerging::LAMPartitionManager& manager4 = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(copy3.getMemoryManager());

    EXPECT_EQ(manager4.getNumEntries(), startingEntries);
    EXPECT_EQ(manager4.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    for (size_t i = 0; i < manager4.getPartitionsRef().size() - 1; ++i)
    {
        EXPECT_EQ(manager4.getPartitionsRef()[i].getBlocksRef().size(), 100);;
        for (size_t j = 0; j < manager4.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
            EXPECT_DOUBLE_EQ(manager4.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
    }

    delete am;
    delete copy;
}

GTEST_TEST(lazyAdaptiveMerging, insert)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;

    const size_t reorganizationBlocksTreshold = 100;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    {
        const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

        EXPECT_EQ(manager.getNumEntries(), startingEntries);
        EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

        for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
        {
            EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
            for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
                EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
        }

    }

    EXPECT_GT(am->insertEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    {
        const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

        EXPECT_EQ(manager.getNumEntries(), startingEntries);
        EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

        for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
        {
            EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
            for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
                EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
        }

    }

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, bulkload)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;

    const size_t reorganizationBlocksTreshold = 100;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    {
        const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

        EXPECT_EQ(manager.getNumEntries(), startingEntries);
        EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

        for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
        {
            EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
            for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
                EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
        }

    }

    EXPECT_GT(am->bulkloadEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    {
        const LazyAdaptiveMerging::LAMPartitionManager& manager = dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager());

        EXPECT_EQ(manager.getNumEntries(), startingEntries);
        EXPECT_EQ(manager.getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

        for (size_t i = 0; i < manager.getPartitionsRef().size() - 1; ++i)
        {
            EXPECT_EQ(manager.getPartitionsRef()[i].getBlocksRef().size(), 100);;
            for (size_t j = 0; j < manager.getPartitionsRef()[i].getBlocksRef().size() - 1; ++j)
                EXPECT_DOUBLE_EQ(manager.getPartitionsRef()[i].getBlocksRef()[j].usage(), 1.0);
        }

    }

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, singleFindPoint)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 10;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    EXPECT_GT(am->findPointEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, (rWriteTime + rReadTime));
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, singleFindRange)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 100;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 10;
    const size_t reorganizationMaxBlocks = 10000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(1)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, (rWriteTime + rReadTime));
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, singleBigFindRange)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 10;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 1;
    const size_t reorganizationMaxBlocks = 10000000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    const size_t numOperations = 20000;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(numOperations)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, (rReadTime + rWriteTime));

    const size_t minPagesForEntries = (numOperations * recordSize + disk->getLowLevelController().getPageSize() - 1) / disk->getLowLevelController().getPageSize();
    EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, minPagesForEntries* rReadTime);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_GE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 1);
    EXPECT_LE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, fullFindPoint)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 10;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 1;
    const size_t reorganizationMaxBlocks = 1000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    const size_t findInOperation = 100;
    size_t i = findInOperation;
    while (dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_GT(am->findPointEntries(static_cast<size_t>(findInOperation)), 0.0);

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        i += findInOperation;

        EXPECT_GE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
        if (dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization() > 0)
        {
            EXPECT_LE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);
        }
        else
        {
            EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);
            EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
        }
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}


GTEST_TEST(lazyAdaptiveMerging, fullFindRange)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 10;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 5;
    const size_t reorganizationMaxBlocks = 1000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    size_t i = 1;
    while (dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_GT(am->findRangeEntries(0.05), 0.0);

        std::cout << "Entries LEFT " <<dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() << std::endl;

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        EXPECT_GE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
        if (dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization() > 0)
        {
            EXPECT_LE(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);
        }
        else
        {
            EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);
            EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
        }

        ++i;
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, singleDelete)
{
 Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 10;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 5;
    const size_t reorganizationMaxBlocks = 1000;
    const double lebUsageTreshold = 0.9;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    EXPECT_GT(am->deleteEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - 1);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_GT(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0.0);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    delete am;
}

GTEST_TEST(lazyAdaptiveMerging, singleBigDelete)
{
 Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize() * 10;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numLebs = (startingEntries + ( disk->getLowLevelController().getBlockSize() / recordSize) - 1) / ( disk->getLowLevelController().getBlockSize() / recordSize);

    const size_t reorganizationBlocksTreshold = 1;
    const size_t reorganizationMaxBlocks = 1000;
    const double lebUsageTreshold = 0.95;

    const size_t numOperations = 10000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new LazyAdaptiveMerging(index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold);

    // const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 0);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    EXPECT_GT(am->deleteEntries(numOperations), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_GT(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_GT(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);
    EXPECT_EQ(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfRerganization(), 1);
    EXPECT_LT(dynamic_cast<const LazyAdaptiveMerging::LAMPartitionManager&>(dynamic_cast<LazyAdaptiveMerging*>(am)->getMemoryManager()).getNumberOfLebs(), numLebs);

    delete am;
}