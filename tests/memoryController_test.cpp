#include <storage/memoryController.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

class MemoryModelTest : public MemoryModel
{
private:
    double readTime;
    double writeTime;

public:
    ~MemoryModelTest() = default;
    MemoryModelTest() = default;
    MemoryModelTest(const MemoryModelTest&) = default;
    MemoryModelTest& operator=(const MemoryModelTest&) = default;
    MemoryModelTest(MemoryModelTest &&) = default;
    MemoryModelTest& operator=(MemoryModelTest &&) = default;

    MemoryModelTest(const char* modelName,
                    size_t pageSize,
                    double readTime,
                    double writeTime)
    : MemoryModel(modelName, pageSize, 0), readTime{readTime}, writeTime{writeTime}
    {

    }

    std::string toStringFull(bool oneLine = true) const noexcept(true) override
    {
        if (oneLine)
            return std::string(std::string("MemoryModel {") +
                               std::string(" .name = ") + std::string(modelName) +
                               std::string(" .pageSize = ") + std::to_string(pageSize) +
                               std::string(" .blockSize = ") + std::to_string(blockSize) +
                               std::string(" .touchedBytes = ") + std::to_string(touchedBytes) +
                               std::string(" .readTime = ") + std::to_string(readTime) +
                               std::string(" .writeTime = ") + std::to_string(writeTime) +
                               std::string(" }"));
        else
            return std::string(std::string("MemoryModel {\n") +
                               std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                               std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                               std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                               std::string("\t.touchedBytes = ") + std::to_string(touchedBytes) + std::string("\n") +
                               std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                               std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                               std::string("}"));
    }

    double writeBytes(size_t bytes) noexcept(true) override
    {
        const size_t pages = bytesToPages(bytes);

        touchedBytes += pages * pageSize;
        return static_cast<double>(pages) * writeTime;
    }

    double overwriteBytes(size_t bytes) noexcept(true)  override
    {
        return 100.0 * writeBytes(bytes);
    }

    double readBytes(size_t bytes) noexcept(true) override
    {
        const size_t pages = bytesToPages(bytes);

        return static_cast<double>(pages) * readTime;
    }
};

GTEST_TEST(generalControllerBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);
}

GTEST_TEST(generalControllerBasicTest, singleRead)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 0), 0.0);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
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

GTEST_TEST(generalControllerBasicTest, severalReads)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

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

GTEST_TEST(generalControllerBasicTest, singleWrite)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

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

GTEST_TEST(generalControllerBasicTest, severalWrites)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

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

GTEST_TEST(generalControllerBasicTest, singleOverwrite)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    const double overwriteTime = writeTime * 100.0;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), overwriteTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    // Pages in write QUEUE: [1 - 1]
    addr = pageSize + 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 10), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    // next page touched
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, pageSize - 5), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * overwriteTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * overwriteTime + readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * overwriteTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);
}

GTEST_TEST(generalControllerBasicTest, severalOverwrites)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    const double overwriteTime = writeTime * 100.0;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

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
    EXPECT_DOUBLE_EQ(controller.flushCache(), 7.0 * overwriteTime + 4.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), 7 * pageSize);
}

GTEST_TEST(generalControllerBasicTest, mixWorkload)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    const double overwriteTime = writeTime * 100.0;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    addr = pageSize * 100;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);

    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(controller.flushCache(), 1.0 * writeTime +  1.0 * overwriteTime);

    EXPECT_EQ(controller.getMemoryWearOut(), pageSize * 2);
}

GTEST_TEST(generalControllerBasicTest, mixWorkloadDifferentCacheLines)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    const double overwriteTime = writeTime * 100.0;
    const size_t readCacheLineSize = pageSize;
    const size_t writeCacheLineSize = 10 * pageSize;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryController controller(memory, readCacheLineSize, writeCacheLineSize);

    uintptr_t addr = 0;

    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    addr = pageSize * 100;
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + 1000, 100), 0.0);

    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(controller.flushCache(), 10.0 * writeTime +  10.0 * overwriteTime + 9.0 * readTime);

    EXPECT_EQ(controller.getMemoryWearOut(), writeCacheLineSize * 2);
}