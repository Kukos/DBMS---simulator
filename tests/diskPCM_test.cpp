#include <disk/diskPCM.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(diskPCMBasicTest, interface)
{
    DiskPCM* disk = new DiskPCM_DefaultModel();
    delete disk;
}

GTEST_TEST(diskPCMBasicTest, copy)
{
    Disk* disk = new DiskPCM_DefaultModel();
    const double readTime = 50.0 / 1000000000.0;

    uintptr_t addr = 0;

    // cache is disabled in MemoryControllerTest class
    EXPECT_DOUBLE_EQ(disk->readBytes(addr, 10), readTime);
    EXPECT_DOUBLE_EQ(disk->readBytes(addr + 10000, 10), readTime);

    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    EXPECT_EQ(disk->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_DOUBLE_EQ(disk->flushCache(), 0.0);

    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);

    DiskPCM* copy = new DiskPCM(*dynamic_cast<DiskPCM*>(disk));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskPCM copy2(*copy);

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskPCM copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}

GTEST_TEST(diskPCMBasicTest, move)
{
    Disk* disk = new DiskPCM_DefaultModel();
    const double readTime = 50.0 / 1000000000.0;

    uintptr_t addr = 0;

    // cache is disabled in MemoryControllerTest class
    EXPECT_DOUBLE_EQ(disk->readBytes(addr, 10), readTime);
    EXPECT_DOUBLE_EQ(disk->readBytes(addr + 10000, 10), readTime);

    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    EXPECT_EQ(disk->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_DOUBLE_EQ(disk->flushCache(), 0.0);

    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);

    DiskPCM* copy = new DiskPCM(std::move(*dynamic_cast<DiskPCM*>(disk)));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskPCM copy2(std::move(*copy));

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskPCM copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 20);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}