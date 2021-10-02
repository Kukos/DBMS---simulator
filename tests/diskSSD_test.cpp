#include <disk/diskSSD.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(diskSSDBasicTest, interface)
{
    DiskSSD* disk = new DiskSSD_Samsung840();
    delete disk;

    disk = new DiskSSD_IntelDCP4511();
    delete disk;

    disk = new DiskSSD_ToshibaVX500();
    delete disk;
}

GTEST_TEST(diskSSDBasicTest, copy)
{
    Disk* disk = new DiskSSD_Samsung840();
    const double readTime = 21.0 / 1000000.0;

    uintptr_t addr = 0;

    // cache is disabled in MemoryControllerTest class
    EXPECT_DOUBLE_EQ(disk->readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(disk->readBytes(addr + 10000, 100), readTime);

    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    EXPECT_EQ(disk->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_DOUBLE_EQ(disk->flushCache(), 0.0);

    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);

    DiskSSD* copy = new DiskSSD(*dynamic_cast<DiskSSD*>(disk));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskSSD copy2(*copy);

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskSSD copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}

GTEST_TEST(diskSSDBasicTest, move)
{
    Disk* disk = new DiskSSD_Samsung840();
    const double readTime = 21.0 / 1000000.0;

    uintptr_t addr = 0;

    // cache is disabled in MemoryControllerTest class
    EXPECT_DOUBLE_EQ(disk->readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(disk->readBytes(addr + 10000, 100), readTime);

    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    EXPECT_EQ(disk->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_DOUBLE_EQ(disk->flushCache(), 0.0);

    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);
    EXPECT_DOUBLE_EQ(disk->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);

    DiskSSD* copy = new DiskSSD(std::move(*dynamic_cast<DiskSSD*>(disk)));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskSSD copy2(std::move(*copy));

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskSSD copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}