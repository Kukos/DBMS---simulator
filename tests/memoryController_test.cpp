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

    MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelTest(*this);
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

    // try to read some counters
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);
}

GTEST_TEST(generalControllerBasicTest, copy)
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
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_DOUBLE_EQ(controller.readBytes(0, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController* copy = new MemoryController(controller);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), 0);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController copy2(*copy);

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), 0);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    delete copy;
}

GTEST_TEST(generalControllerBasicTest, move)
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
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_DOUBLE_EQ(controller.readBytes(0, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController* copy = new MemoryController(std::move(controller));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), 0);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy->getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController copy2(std::move(*copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    MemoryController copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), 0);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(copy3.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime);

    delete copy;
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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    // Fetched pages: [0 - 0]
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), readTime);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr + pageSize / 2, pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(controller.readBytes(addr, 2000), 0.0);
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
    EXPECT_DOUBLE_EQ(controller.flushCache(), writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (1 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 3.0 * writeTime);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (1 + 2 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 5.0 * writeTime);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.writeBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * writeTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, (1 + 2 + 2 + 99) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 104.0 * writeTime);
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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 7 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 7.0 * writeTime);
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
    EXPECT_DOUBLE_EQ(controller.flushCache(), overwriteTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, overwriteTime);

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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (1 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 3.0 * overwriteTime);

    // Pages in write QUEUE: [10 - 11]
    addr = pageSize * 10;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 4000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 3000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 2.0 * overwriteTime + readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (1 + 2 + 2) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 5.0 * overwriteTime);

    // Pages in write QUEUE: [4 - 102]
    addr = 10000;
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 200000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 100), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr, 2000), 0.0);
    EXPECT_DOUBLE_EQ(controller.overwriteBytes(addr + 1000, 100), 0.0);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2) * pageSize);
    EXPECT_DOUBLE_EQ(controller.flushCache(), 99.0 * overwriteTime + 2.0 * readTime);
    EXPECT_EQ(controller.getMemoryWearOut(), (1 + 2 + 2 + 99) * pageSize);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, (1 + 2 + 2 + 99) * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 4);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 104.0 * overwriteTime);
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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 7 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 7.0 * overwriteTime);
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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 2 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0 * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, overwriteTime);
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

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 11 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 3);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 11.0 * readTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 10 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 10.0 * writeTime);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 10 * pageSize);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 10.0 * overwriteTime);
}

GTEST_TEST(generalControllerBasicTest, resetState)
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

    controller.resetState();

    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 0.0);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second, 0.0);
}