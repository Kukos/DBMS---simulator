#include <storage/memoryControllerFlashNandFTL.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(flashNandFTLControllerBasicTest, interface)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    MemoryModel* generalMemory = new MemoryModelFlashNandFTL_SamsungK9F1G08U0D();
    MemoryController generalController(generalMemory);
    EXPECT_EQ(std::string(generalController.getModelName()), std::string("FlashNandFTL:samsungK9F1G08U0D"));
    EXPECT_EQ(generalController.getPageSize(), 2048);
    EXPECT_EQ(generalController.getBlockSize(), 2048 * 32);
    EXPECT_EQ(generalController.getMemoryWearOut(), 0);
}

GTEST_TEST(flashNandFTLControllerBasicTest, copy)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerFlashNandFTL(controller);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerFlashNandFTL copy2(*dynamic_cast<MemoryControllerFlashNandFTL*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerFlashNandFTL copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(flashNandFTLControllerBasicTest, move)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerFlashNandFTL(std::move(controller));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerFlashNandFTL copy2(std::move(*dynamic_cast<MemoryControllerFlashNandFTL*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerFlashNandFTL copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(flashNandFTLControllerBasicTest, singleRead)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

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

    // Fetched pages: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 4000), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 200000), 99 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(flashNandFTLControllerBasicTest, severalReads)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    uintptr_t addr = 0;

    // Fetched pages: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 10, pageSize - 5), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7], [1 - 6], [8 - 9]}
    addr = 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10 * pageSize), 8.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(flashNandFTLControllerBasicTest, singleWrite)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(flashNandFTLControllerBasicTest, severalWrites)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), 7.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}

GTEST_TEST(flashNandFTLControllerBasicTest, singleOverwrite)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime + readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeTime + 3.0 * eraseTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(flashNandFTLControllerBasicTest, severalOverwrites)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandFTL* memory = new MemoryModelFlashNandFTL(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    MemoryControllerFlashNandFTL controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), 7.0 * writeTime + 4.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}