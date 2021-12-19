#include <disk/diskColumnOverlay.hpp>
#include <storage/memoryControllerColumnOverlay.hpp>
#include <string>
#include <iostream>
#include <storage/memoryModelPCM.hpp>

#include <gtest/gtest.h>

class MemoryControllerTest : public MemoryController
{
private:
    double readTime;
    double writeTime;
    size_t writeQueueSize;
    size_t overwriteQueueSize;

    size_t bytesToPages(size_t bytes) const noexcept(true)
    {
        const size_t pageSize = memoryModel->getPageSize();
        return (bytes + (pageSize - 1)) / pageSize;
    }

public:
    ~MemoryControllerTest() = default;
    MemoryControllerTest() = default;
    MemoryControllerTest(const MemoryControllerTest&) = default;
    MemoryControllerTest& operator=(const MemoryControllerTest&) = default;
    MemoryControllerTest(MemoryControllerTest &&) = default;
    MemoryControllerTest& operator=(MemoryControllerTest &&) = default;

    MemoryControllerTest(const char* modelName,
                         size_t pageSize,
                         double readTime,
                         double writeTime)
    : MemoryController(new MemoryModelPCM(modelName, pageSize, readTime, writeTime)), readTime{readTime}, writeTime{writeTime}, writeQueueSize{0}, overwriteQueueSize{0}
    {

    }

    MemoryController* clone() const noexcept(true) override
    {
        return new MemoryControllerTest(*this);
    }

    double flushCache() noexcept(true)
    {
        const double writeTime = writeQueueSize * this->writeTime;
        const double overwriteTime = overwriteQueueSize * this->writeTime;

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, writeTime);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, overwriteTime);

        writeQueueSize = 0;
        overwriteQueueSize = 0;

        return writeTime + overwriteTime;
    }

    double readBytes(uintptr_t addr, size_t bytes) noexcept(true) override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = static_cast<double>(pages) * readTime;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, time);

        return time;
    }

    double writeBytes(uintptr_t addr, size_t bytes) noexcept(true) override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = 0.0;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, time);

        writeQueueSize += pages;

        return time;
    }

    double overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true)  override
    {
        (void)addr;

        const size_t pages = bytesToPages(bytes);
        const double time = 0.0;

        // make operation visible via fake counters
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, bytes);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, time);

        overwriteQueueSize += pages;
        return time;
    }
};

GTEST_TEST(diskColumnOverlayBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    EXPECT_EQ(std::string(disk.getLowLevelController().getModelName()), std::string(modelName));
    EXPECT_EQ(disk.getLowLevelController().getPageSize(), pageSize);
    EXPECT_EQ(disk.getLowLevelController().getBlockSize(), 0);
    EXPECT_EQ(disk.getLowLevelController().getMemoryWearOut(), 0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; id < MemoryCounters::MEMORY_COUNTER_D_MAX_ITERATOR; ++id)
        EXPECT_DOUBLE_EQ(disk.getDiskCounter(id).second, 0.0);

    for (auto id = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; id < MemoryCounters::MEMORY_COUNTER_L_MAX_ITERATOR; ++id)
        EXPECT_EQ(disk.getDiskCounter(id).second, 0L);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    // cache is disabled in MemoryControllerTest class
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS,  2);
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, readTime * 2.0);

    dynamic_cast<MemoryControllerColumnOverlay*>(&const_cast<MemoryController&>(disk.getLowLevelController()))->pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 2);
    EXPECT_EQ(disk.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    DiskColumnOverlay copy(disk);

    EXPECT_EQ(copy.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(copy.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskColumnOverlay copy2;
    copy2 = copy;

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    // cache is disabled in MemoryControllerTest class
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS,  2);
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, readTime * 2.0);

    dynamic_cast<MemoryControllerColumnOverlay*>(&const_cast<MemoryController&>(disk.getLowLevelController()))->pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 2);
    EXPECT_EQ(disk.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);

    DiskColumnOverlay copy(std::move(disk));

    EXPECT_EQ(copy.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(copy.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    DiskColumnOverlay copy2;
    copy2 = std::move(copy);

    EXPECT_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);
    EXPECT_DOUBLE_EQ(copy2.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, readTime * 2);
    EXPECT_EQ(copy2.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, set)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    disk.setCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 5);
    disk.setCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 2.0);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 5);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, peg)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 5);
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 2.0);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 5);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, resetState)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    DiskColumnOverlay disk(memoryController);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);

    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 5);
    disk.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, 2.0);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 5);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 2.0);

    dynamic_cast<MemoryControllerColumnOverlay*>(&const_cast<MemoryController&>(disk.getLowLevelController()))->pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 2);
    EXPECT_EQ(disk.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 2);

    disk.resetState();

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);
    EXPECT_DOUBLE_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME).second, 0.0);
    EXPECT_EQ(disk.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS).second, 0);

    delete memoryController;
}

GTEST_TEST(diskColumnOverlayBasicTest, addStats)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 2048;
    const double readTime = 0.1;
    const double writeTime = 4.4;
    MemoryControllerTest* memoryController = new MemoryControllerTest(modelName, pageSize, readTime, writeTime);

    Disk disk(memoryController);
    EXPECT_DOUBLE_EQ(disk.writeBytes(0, 100), 0.0);
    EXPECT_DOUBLE_EQ(disk.flushCache(), writeTime);

    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(disk.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);
    EXPECT_EQ(disk.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);

    DiskColumnOverlay diskOverlay(&disk);
    EXPECT_EQ(diskOverlay.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);
    EXPECT_EQ(diskOverlay.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, writeTime);
    EXPECT_EQ(diskOverlay.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 1);

    diskOverlay.addStats(disk);
    EXPECT_EQ(diskOverlay.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2 * 1);
    EXPECT_EQ(diskOverlay.getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second, 2.0 * writeTime);
    EXPECT_EQ(diskOverlay.getLowLevelController().getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS).second, 2 * 1);
}