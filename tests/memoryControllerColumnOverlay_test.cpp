#include <storage/memoryControllerColumnOverlay.hpp>
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

GTEST_TEST(controllerColumnOverlayBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryControllerColumnOverlay controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // try to read some counters
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    MemoryControllerColumnOverlay controller2(&controller);
    EXPECT_EQ(std::string(controller2.getModelName()), std::string(modelName));
    EXPECT_EQ(controller2.getPageSize(), pageSize);
    EXPECT_EQ(controller2.getBlockSize(), 0);
    EXPECT_EQ(controller2.getMemoryWearOut(), 0);

    // try to read some counters
    EXPECT_EQ(controller2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    delete memory;
}

GTEST_TEST(controllerColumnOverlayBasicTest, pegCounter)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryControllerColumnOverlay controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, 10);
    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 5.5);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    controller.setCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, 5);
    controller.setCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 10.5);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 5);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 10.5);

    delete memory;
}

GTEST_TEST(controllerColumnOverlayBasicTest, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryControllerColumnOverlay controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, 10);
    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 5.5);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    MemoryControllerColumnOverlay copy(controller);

    EXPECT_EQ(std::string(copy.getModelName()), std::string(modelName));
    EXPECT_EQ(copy.getPageSize(), pageSize);
    EXPECT_EQ(copy.getBlockSize(), 0);
    EXPECT_EQ(copy.getMemoryWearOut(), 0);

    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    MemoryControllerColumnOverlay copy2;
    copy2 = copy;

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    delete memory;
}

GTEST_TEST(controllerColumnOverlayBasicTest, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryControllerColumnOverlay controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, 10);
    controller.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 5.5);

    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    MemoryControllerColumnOverlay copy(std::move(controller));

    EXPECT_EQ(std::string(copy.getModelName()), std::string(modelName));
    EXPECT_EQ(copy.getPageSize(), pageSize);
    EXPECT_EQ(copy.getBlockSize(), 0);
    EXPECT_EQ(copy.getMemoryWearOut(), 0);

    EXPECT_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(copy.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    MemoryControllerColumnOverlay copy2;
    copy2 = std::move(copy);

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 10);
    EXPECT_DOUBLE_EQ(copy2.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 5.5);

    delete memory;
}

GTEST_TEST(controllerColumnOverlayBasicTest, changeMemoryModel)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* memory = new MemoryModelTest(modelName, pageSize, readTime, writeTime);
    MemoryControllerColumnOverlay controller(memory);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName));
    EXPECT_EQ(controller.getPageSize(), pageSize);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // try to read some counters
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    const char* const modelName2 = "testModelCHANGED";
    MemoryModel* memory2 = new MemoryModelTest(modelName2, pageSize * 10, readTime * 2, writeTime / 2);
    controller.setMemoryModel(*memory2);

    EXPECT_EQ(std::string(controller.getModelName()), std::string(modelName2));
    EXPECT_EQ(controller.getPageSize(), pageSize * 10);
    EXPECT_EQ(controller.getBlockSize(), 0);
    EXPECT_EQ(controller.getMemoryWearOut(), 0);

    // try to read some counters
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES).second, 0);
    EXPECT_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(controller.getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    delete memory;
    delete memory2;
}