#include <storage/memoryModelColumnOverlay.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(memoryModelColumnOverlayBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    MemoryModel* model2 = new MemoryModelColumnOverlay(model);

    EXPECT_EQ(std::string(model2->getModelName()), std::string(modelName));
    EXPECT_EQ(model2->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model2->getMemoryWearOut(), 0);

    delete model;
    delete model2;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    dynamic_cast<MemoryModelColumnOverlay*>(model)->setWearOut(10);

    MemoryModel* copy = new MemoryModelColumnOverlay(*dynamic_cast<MemoryModelColumnOverlay*>(model));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 10);

    MemoryModelColumnOverlay copy2(*dynamic_cast<MemoryModelColumnOverlay*>(model));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 10);

    MemoryModelColumnOverlay copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 10);

    delete model;
    delete copy;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    dynamic_cast<MemoryModelColumnOverlay*>(model)->setWearOut(10);

    MemoryModel* copy = new MemoryModelColumnOverlay(std::move(*dynamic_cast<MemoryModelColumnOverlay*>(model)));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 10);

    MemoryModelColumnOverlay copy2(std::move(*dynamic_cast<MemoryModelColumnOverlay*>(model)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 10);

    MemoryModelColumnOverlay copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 10);

    delete model;
    delete copy;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, read)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(model->readBytes(1), 0.0);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize / 2), 0.0);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize), 0.0);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize + 1), 0.0);
    EXPECT_DOUBLE_EQ(model->readBytes(3 * pageSize), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    delete model;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, write)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(model->writeBytes(1), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize / 2), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize + 1), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->writeBytes(3 * pageSize), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    delete model;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, overwrite)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModel* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(1), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize / 2), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize + 1), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(3 * pageSize), 0.0);
    EXPECT_EQ(model->getMemoryWearOut(), 0.0);

    delete model;
}

GTEST_TEST(memoryModelColumnOverlayBasicTest, touchedBytes)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const size_t blockSize = 100;

    MemoryModelColumnOverlay* model = new MemoryModelColumnOverlay(modelName, pageSize, blockSize);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), blockSize);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    model->setWearOut(100);
    EXPECT_EQ(model->getMemoryWearOut(), 100);

    model->addWearOut(123);
    EXPECT_EQ(model->getMemoryWearOut(), 100 + 123);

    model->resetWearOut();
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    delete model;
}