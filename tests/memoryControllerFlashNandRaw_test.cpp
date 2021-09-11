#include <storage/memoryControllerFlashNandRaw.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(flashNandRawControllerBasicTest, interface)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(controller.getCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(controller.getCounter(id).second, 0L);

    MemoryModel* generalMemory = new MemoryModelFlashNandRaw_SamsungK9F1G08U0D();
    MemoryController generalController(generalMemory);
    EXPECT_EQ(std::string(generalController.getModelName()), std::string("FlashNandRaw:samsungK9F1G08U0D"));
    EXPECT_EQ(generalController.getPageSize(), 2048);
    EXPECT_EQ(generalController.getBlockSize(), 2048 * 32);
    EXPECT_EQ(generalController.getMemoryWearOut(), 0);
}

GTEST_TEST(flashNandRawControllerBasicTest, copy)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerFlashNandRaw(controller);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerFlashNandRaw copy2(*dynamic_cast<MemoryControllerFlashNandRaw*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerFlashNandRaw copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(flashNandRawControllerBasicTest, move)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerFlashNandRaw(std::move(controller));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerFlashNandRaw copy2(std::move(*dynamic_cast<MemoryControllerFlashNandRaw*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerFlashNandRaw copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(flashNandRawControllerBasicTest, singleRead)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    // Fetched pages: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, pageSize - 5), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 3.0 * readTime);

    // Fetched pages: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 4000), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.0 * readTime);

    // Fetched pages: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 200000), 99 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2 + 2 + 99) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 5);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 104.0 * readTime);
}

GTEST_TEST(flashNandRawControllerBasicTest, severalReads)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    // Fetched pages: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    // Fetched pages: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 10, pageSize - 5), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 3.0 * readTime);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2 + 1) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 4.0 * readTime);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7], [1 - 6], [8 - 9]}
    addr = 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10 * pageSize), 8.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (1 + 2 + 1 + 8) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 6);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 12.0 * readTime);
}

GTEST_TEST(flashNandRawControllerBasicTest, singleWrite)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 32 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 32.0 * writeTime);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (32 + 32) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 64.0 * writeTime);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (32 + 32 + 32) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 96.0 * writeTime);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 128.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32 + 128) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (32 + 32 + 32 + 128) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 224.0 * writeTime);
}


GTEST_TEST(flashNandRawControllerBasicTest, severalWrites)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    // Pages in write QUEUE: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 10, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7], [5 - 6], [7 - 8]}
    addr = 5 * pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4 * pageSize - 100), 0.0);

    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Flush cost: 0 - 0, 10 - 11
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 32 * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 32 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 32.0 * writeTime);
}

GTEST_TEST(flashNandRawControllerBasicTest, singleOverwrite)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime + 31.0 * readTime + eraseTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 31 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 31.0 * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 32 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 32.0 * writeTime + eraseTime);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime + 32.0 * readTime + eraseTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (31 + 32) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, (31.0 + 32.0) * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (32 + 32) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, (32.0 + 32.0) * writeTime + (1.0 + 1.0) * eraseTime);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime + 31.0 * readTime + eraseTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (31 + 32 + 31) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, (31.0 + 32.0 + 31.0) * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (32 + 32 + 32) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, (32.0 + 32.0 + 32.0) * writeTime + (1.0 + 1.0 + 1.0) * eraseTime);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 128.0 * writeTime + 4.0 * eraseTime + 31.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (32 + 32 + 32 + 128) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, (31 + 32 + 31 + 31.0) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, (31.0 + 32.0 + 31.0 + 31.0) * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (32 + 32 + 32 + 128) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, (32.0 + 32.0 + 32.0 + 128.0) * writeTime + (1.0 + 1.0 + 1.0 + 4.0) * eraseTime);
}

GTEST_TEST(flashNandRawControllerBasicTest, severalOverwrites)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw* memory = new MemoryModelFlashNandRaw(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandRaw controller(memory);

    uintptr_t addr = 0;

    // Pages in write QUEUE: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 10, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7], [5 - 6], [7 - 8]}
    addr = 5 * pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4 * pageSize - 100), 0.0);

    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Flush cost: 0 - 0, 10 - 11
    EXPECT_DOUBLE_EQ(controller.flushCache(), 32.0 * writeTime + 29.0 * readTime + eraseTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 32 * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 29 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 29.0 * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 32 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 32.0 * writeTime + eraseTime);
}