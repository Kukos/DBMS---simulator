#include <storage/memoryControllerPCM.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(pcmControllerBasicTest, interface)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    MemoryModel* generalMemory = new MemoryModelPCM_DefaultModel();
    MemoryController generalController(generalMemory);
    EXPECT_EQ(std::string(generalController.getModelName()), std::string("PCM:defaultModel"));
    EXPECT_EQ(generalController.getPageSize(), 64);
    EXPECT_EQ(generalController.getBlockSize(), 0);
    EXPECT_EQ(generalController.getMemoryWearOut(), 0);
}

GTEST_TEST(pcmControllerBasicTest, copy)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerPCM(controller);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerPCM copy2(*dynamic_cast<MemoryControllerPCM*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerPCM copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(pcmControllerBasicTest, move)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerPCM(std::move(controller));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerPCM copy2(std::move(*dynamic_cast<MemoryControllerPCM*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerPCM copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(pcmControllerBasicTest, singleRead)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 4), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 4), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 2, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [1 - 1]
    addr = pageSize + 2;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 1), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 3), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, pageSize - 1), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1, 5), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 5, 2), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 6300), 99 * readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 3, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(pcmControllerBasicTest, severalReads)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    // Fetched pages: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 30), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 50), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 30, 30), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 11), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 10, pageSize - 5), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 22), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 12, 31), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 21), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 41, 11), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7], [1 - 6], [8 - 9]}
    addr = 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10 * pageSize), 8.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(pcmControllerBasicTest, singleWrite)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 20), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 30, 7), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 20), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 60, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 100, 8), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 6300), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 3, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(pcmControllerBasicTest, severalWrites)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    // Pages in write QUEUE: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 3), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 50, 10), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 50), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 10, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 60), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1, 60), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 61), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 31, 17), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7], [5 - 6], [7 - 8]}
    addr = 5 * pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4 * pageSize - 15), 0.0);

    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Flush cost: 0 - 0, 10 - 11
    EXPECT_DOUBLE_EQ(controller.flushCache(), 7.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}

GTEST_TEST(pcmControllerBasicTest, singleOverwrite)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Pages in write QUEUE: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 20), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 30, 7), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeTime + readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 20), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 60, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 100, 8), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime + readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 6300), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3, 3), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(pcmControllerBasicTest, severalOverwrites)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 64;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM* memory = new MemoryModelPCM(modelName, pageSize, readTime, writeTime);
    MemoryControllerPCM controller(memory);

    uintptr_t addr = 0;

    // Pages in write QUEUE: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 1), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 3), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 50, 10), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 50), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 10, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 60), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1, 60), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 11), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 61), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 31, 17), 0.0);

    // Pages in write QUEUE: {[0 - 0], [10 - 11], [7 - 7], [5 - 6], [7 - 8]}
    addr = 5 * pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4 * pageSize - 15), 0.0);

    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Flush cost: 0 - 0, 10 - 11
    EXPECT_DOUBLE_EQ(controller.flushCache(), 7.0 * writeTime + 4.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}