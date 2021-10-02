#include <disk/diskFlashNandRaw.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(diskFlashNandRawBasicTest, interface)
{
    DiskFlashNandRaw* disk = new DiskFlashNandRaw_SamsungK9F1G08U0D();
    delete disk;

    disk = new DiskFlashNandRaw_MicronMT29F32G08ABAAA();
    delete disk;

    disk = new DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1();
    delete disk;
}

GTEST_TEST(diskFlashNandRawBasicTest, copy)
{
    Disk* disk = new DiskFlashNandRaw_SamsungK9F1G08U0D();
    const double readTime = 35.0 / 1000000.0;

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

    DiskFlashNandRaw* copy = new DiskFlashNandRaw(*dynamic_cast<DiskFlashNandRaw*>(disk));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskFlashNandRaw copy2(*copy);

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskFlashNandRaw copy3;
    copy3 = copy2;

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}

GTEST_TEST(diskFlashNandRawBasicTest, move)
{
    Disk* disk = new DiskFlashNandRaw_SamsungK9F1G08U0D();
    const double readTime = 35.0 / 1000000.0;

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

    DiskFlashNandRaw* copy = new DiskFlashNandRaw(std::move(*dynamic_cast<DiskFlashNandRaw*>(disk)));

    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy->getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy->getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskFlashNandRaw copy2(std::move(*copy));

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskFlashNandRaw copy3;
    copy3 = std::move(copy2);

    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 200);
    EXPECT_DOUBLE_EQ(copy3.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy3.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete disk;
    delete copy;
}