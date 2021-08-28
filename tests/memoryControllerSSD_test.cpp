#include <storage/memoryControllerSSD.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(ssdControllerBasicTest, interface)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    MemoryModel* generalMemory = new MemoryModelSSD_Samsung840();
    MemoryController generalController(generalMemory);
    EXPECT_EQ(std::string(generalController.getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(generalController.getPageSize(), 8192);
    EXPECT_EQ(generalController.getBlockSize(), 8192 * 64);
    EXPECT_EQ(generalController.getMemoryWearOut(), 0);
}

GTEST_TEST(ssdControllerBasicTest, copy)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerSSD(controller);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerSSD copy2(*dynamic_cast<MemoryControllerSSD*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerSSD copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(ssdControllerBasicTest, move)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), blockSize);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_EQ(controller.getPageSize(), pageSize);

    MemoryController* copy = new MemoryControllerSSD(std::move(controller));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);

    MemoryControllerSSD copy2(std::move(*dynamic_cast<MemoryControllerSSD*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);

    MemoryControllerSSD copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);

    delete copy;
}

GTEST_TEST(ssdControllerBasicTest, singleRead)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, pageSize - 5), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 4000), 2.0 * readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 200000), 99 * readSeqTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(ssdControllerBasicTest, severalReads)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    uintptr_t addr = 0;

    // Fetched pages: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11]}
    addr = 10 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 10, pageSize - 5), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7]}
    addr = 7 * pageSize;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readRandomTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // 0 - 6 SEQ, 8 - 9 RANDOM
    // Fetched pages: {[0 - 0], [10 - 11], [7 - 7], [1 - 6], [8 - 9]}
    addr = 10;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 10 * pageSize), 6.0 * readSeqTime + 2.0 * readRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(ssdControllerBasicTest, singleWrite)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeSeqTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(ssdControllerBasicTest, severalWrites)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

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

    // Flush cost: 0 - 0, 10 - 11 as random SEQ, 5 - 8 SEQ
    EXPECT_DOUBLE_EQ(controller.flushCache(), 3.0 * writeRandomTime + 4.0 * writeSeqTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}

GTEST_TEST(ssdControllerBasicTest, singleOverwrite)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeRandomTime + 2.0 * readRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeRandomTime + readRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeSeqTime + 3.0 * eraseTime + 2.0 * readRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(ssdControllerBasicTest, severalOverwrites)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

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

    // Flush cost: 0 - 0, 10 - 11 as random SEQ, 5 - 8 SEQ
    EXPECT_DOUBLE_EQ(controller.flushCache(), 3.0 * writeRandomTime + 4.0 * writeSeqTime + 4.0 * readRandomTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}

GTEST_TEST(ssdControllerBasicTest, seqReadWhenOverwrite)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD* memory = new MemoryModelSSD(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    MemoryControllerSSD controller(memory);

    uintptr_t addr = 0 * pageSize;

    // Pages in write QUEUE: {[0 - 0]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    addr = 1 * pageSize;

    // Pages in write QUEUE: {[0 - 0], [1 - 1]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    addr = 2 * pageSize;

    // Pages in write QUEUE: {[0 - 0], [1 - 1], [2 - 2]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    addr = 3 * pageSize;

    // Pages in write QUEUE: {[0 - 0], [1 - 1], [2 - 2], [3 - 3]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    addr = 4 * pageSize;

    // Pages in write QUEUE: {[0 - 0], [1 - 1], [2 - 2], [3 - 3], [4 - 4]}
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);

    EXPECT_EQ(controller.getMemoryWearOut(), 0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 5.0 * writeSeqTime + 5.0 * readSeqTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 5 * pageSize);
}