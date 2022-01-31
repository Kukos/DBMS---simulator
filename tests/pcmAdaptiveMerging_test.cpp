#include <adaptiveMerging/pcmAdaptiveMerging.hpp>
#include <disk/diskSSD.hpp>
#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>
#include <index/bptree.hpp>

#include <gtest/gtest.h>

GTEST_TEST(pcmAdaptiveMerging, interfaceSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    DBIndex* copy = am->clone();

    delete copy;
    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, copySSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    PCMAdaptiveMerging* copy = new PCMAdaptiveMerging(*dynamic_cast<PCMAdaptiveMerging*>(am));

    EXPECT_EQ(std::string(copy->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy2(*copy);

    EXPECT_EQ(std::string(copy2.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy3;
    copy3 = copy2;

    EXPECT_EQ(std::string(copy3.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    delete am;
    delete copy;
}

GTEST_TEST(pcmAdaptiveMerging, moveSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    PCMAdaptiveMerging* copy = new PCMAdaptiveMerging(std::move(*dynamic_cast<PCMAdaptiveMerging*>(am)));

    EXPECT_EQ(std::string(copy->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy2(std::move(*copy));

    EXPECT_EQ(std::string(copy2.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(std::string(copy3.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
    delete copy;
}

GTEST_TEST(pcmAdaptiveMerging, insertSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    EXPECT_GT(am->insertEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, bulkloadSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    EXPECT_GT(am->bulkloadEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleFindPointSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findPointEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, rWriteTime + rReadTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);


    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleFindRangeSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(1)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, rReadTime + rWriteTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleBigFindRangeSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numOperations = 10000;

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(numOperations)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, rReadTime + rWriteTime);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, fullFindPointSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    // const size_t numOperations = 10000;

    // const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    size_t i = 100;
    while (dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        EXPECT_GT(am->findPointEntries(static_cast<size_t>(100)), 0.0);

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i);
        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        i += 100;
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}


GTEST_TEST(pcmAdaptiveMerging, fullFindRangeSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    // const size_t numOperations = 10000;

    // const double rReadTime = 21.0 / 1000000.0;
    // const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    size_t i = 1;
    while (dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        EXPECT_GT(am->findRangeEntries(0.1), 0.0);

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        ++i;
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}


GTEST_TEST(pcmAdaptiveMerging, singleDeleteSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->deleteEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - 1);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, rReadTime + rWriteTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * rReadTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleBigDeleteSSD)
{
    Disk* disk = new DiskSSD_Samsung840();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize();
    const size_t partitionSize = disk->getLowLevelController().getBlockSize();
    // const size_t pagesInBlock = disk->getLowLevelController().getBlockSize() / disk->getLowLevelController().getPageSize();
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const size_t numOperations = 10000;

    const double rReadTime = 21.0 / 1000000.0;
    const double rWriteTime = 45.0 / 1000000.0;
    // const double eraseTime = 210.0 * 64.0 / 1000000.0;
    // const double sReadTime = 14.0 / 1000000.0;
    // const double sWriteTime = 15.3 / 1000000.0;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->deleteEntries(numOperations), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, rReadTime + rWriteTime);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}


GTEST_TEST(pcmAdaptiveMerging, interfacePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * 10 * 100;
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    DBIndex* copy = am->clone();

    delete copy;
    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, copyPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * 10 * 100;
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;


    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    PCMAdaptiveMerging* copy = new PCMAdaptiveMerging(*dynamic_cast<PCMAdaptiveMerging*>(am));

    EXPECT_EQ(std::string(copy->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy2(*copy);

    EXPECT_EQ(std::string(copy2.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy3;
    copy3 = copy2;

    EXPECT_EQ(std::string(copy3.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    delete am;
    delete copy;
}

GTEST_TEST(pcmAdaptiveMerging, movePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * 10 * 100;
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    // check how to get some statistics
    EXPECT_EQ(std::string(am->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));


    PCMAdaptiveMerging* copy = new PCMAdaptiveMerging(std::move(*dynamic_cast<PCMAdaptiveMerging*>(am)));

    EXPECT_EQ(std::string(copy->getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(copy)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy2(std::move(*copy));

    EXPECT_EQ(std::string(copy2.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy2)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    PCMAdaptiveMerging copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(std::string(copy3.getName()), std::string("PCMAdaptiveMerging"));
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

    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(&copy3)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
    delete copy;
}

GTEST_TEST(pcmAdaptiveMerging, insertPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * 10 * 100;
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    EXPECT_GT(am->insertEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, bulkloadPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * 10 * 100;
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;
    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize, true);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    EXPECT_GT(am->bulkloadEntries(numOperations), 0.0);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries + numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize));

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleFindPointPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findPointEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, writeTime + readTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * readTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);


    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleFindRangePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(1)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 1);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, readTime + writeTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * readTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleBigFindRangePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->findRangeEntries(static_cast<size_t>(numOperations)), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, numOperations);

    EXPECT_EQ(am->getNumEntries(), startingEntries);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), numOperations);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, readTime + writeTime);
    EXPECT_GT(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, fullFindPointPCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    size_t i = 100;
    while (dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        EXPECT_GT(am->findPointEntries(static_cast<size_t>(100)), 0.0);

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i);
        EXPECT_LE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        i += 100;
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}


GTEST_TEST(pcmAdaptiveMerging, fullFindRangePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 10000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    // const double readTime = 50.0 / 1000000000.0;
    // const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    size_t i = 1;
    while (dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() > 0)
    {
        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        EXPECT_GT(am->findRangeEntries(0.1), 0.0);

        EXPECT_EQ(am->getNumEntries(), startingEntries);
        EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries() + index->getNumEntries(), startingEntries);

        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, 0);
        EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0);

        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, i);
        EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, i);

        EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

        ++i;
    }

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_NE(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    delete am;
}


GTEST_TEST(pcmAdaptiveMerging, singleDeletePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->deleteEntries(), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - 1);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - 1);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, readTime + writeTime);
    EXPECT_NEAR(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, numPartitions * readTime, 0.0001);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}

GTEST_TEST(pcmAdaptiveMerging, singleBigDeletePCM)
{
    Disk* disk = new DiskPCM_DefaultModel();

    const size_t startingEntries = 1000000;
    const size_t keySize = 8;
    const size_t dataSize = 64;
    const size_t recordSize = keySize + dataSize;
    const size_t nodeSize = disk->getLowLevelController().getPageSize() * 10;
    const size_t pagesInPartition = 10 * 100;
    const size_t partitionSize = disk->getLowLevelController().getPageSize() * pagesInPartition;
    const size_t numPartitions =  (startingEntries + (partitionSize / recordSize) - 1) / (partitionSize / recordSize);
    const double readTime = 50.0 / 1000000000.0;
    const double writeTime = 1.0 / 1000000;

    const size_t numOperations = 1000;

    DBIndex* index = new BPTree(disk, keySize, dataSize, nodeSize);
    DBIndex* am = new PCMAdaptiveMerging(index, startingEntries, partitionSize);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    EXPECT_GT(am->deleteEntries(numOperations), 0.0);
    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second, 0);

    EXPECT_EQ(am->getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager().getNumEntries(), startingEntries - numOperations);
    EXPECT_EQ(index->getNumEntries(), 0);

    EXPECT_DOUBLE_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second, readTime + writeTime);

    EXPECT_NE(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(am->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second, 1);

    EXPECT_EQ(dynamic_cast<const PCMAdaptiveMerging::AMPartitionManager&>(dynamic_cast<PCMAdaptiveMerging*>(am)->getMemoryManager()).getPartitionsRef().size(), numPartitions);

    delete am;
}
