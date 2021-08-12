#include <storage/memoryModel.hpp>
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

GTEST_TEST(generalMemoryModelBasicTest, interface)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), 0);
    EXPECT_EQ(model->getMemoryWearOut(), 0);
    EXPECT_EQ(model->toString(false), std::string(std::string("MemoryModel {\n") +
                                      std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                      std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                      std::string("\t.blockSize = ") + std::to_string(0) + std::string("\n") +
                                      std::string("}")));

    EXPECT_EQ(model->toStringFull(false), std::string(std::string("MemoryModel {\n") +
                                          std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                          std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                          std::string("\t.blockSize = ") + std::to_string(0) + std::string("\n") +
                                          std::string("\t.touchedBytes = ") + std::to_string(0) + std::string("\n") +
                                          std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                                          std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                                          std::string("}")));

    EXPECT_EQ(model->toString(),  std::string(std::string("MemoryModel {") +
                                              std::string(" .name = ") + std::string(modelName) +
                                              std::string(" .pageSize = ") + std::to_string(pageSize) +
                                              std::string(" .blockSize = ") + std::to_string(0) +
                                              std::string(" }")));

    EXPECT_EQ(model->toStringFull(),  std::string(std::string("MemoryModel {") +
                                                  std::string(" .name = ") + std::string(modelName) +
                                                  std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                  std::string(" .blockSize = ") + std::to_string(0) +
                                                  std::string(" .touchedBytes = ") + std::to_string(0) +
                                                  std::string(" .readTime = ") + std::to_string(readTime) +
                                                  std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                  std::string(" }")));

    delete model;
}

GTEST_TEST(generalMemoryModelBasicTest, copy)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), 0);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(model->readBytes(1), readTime);
    EXPECT_DOUBLE_EQ(model->writeBytes(1), writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1) * pageSize);

    MemoryModel* copy = new MemoryModelTest(*dynamic_cast<MemoryModelTest*>(model));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), 0);
    EXPECT_EQ(copy->getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy->toString(), std::string(std::string("MemoryModel {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(0) +
                                            std::string(" }")));

    EXPECT_EQ(copy->toStringFull(), std::string(std::string("MemoryModel {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(0) +
                                                std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                std::string(" .readTime = ") + std::to_string(readTime) +
                                                std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                std::string(" }")));

    MemoryModelTest copy2(*dynamic_cast<MemoryModelTest*>(model));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy2.toString(),  std::string(std::string("MemoryModel {") +
                                             std::string(" .name = ") + std::string(modelName) +
                                             std::string(" .pageSize = ") + std::to_string(pageSize) +
                                             std::string(" .blockSize = ") + std::to_string(0) +
                                             std::string(" }")));

    EXPECT_EQ(copy2.toStringFull(),  std::string(std::string("MemoryModel {") +
                                                 std::string(" .name = ") + std::string(modelName) +
                                                 std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                 std::string(" .blockSize = ") + std::to_string(0) +
                                                 std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                 std::string(" .readTime = ") + std::to_string(readTime) +
                                                 std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                 std::string(" }")));


    MemoryModelTest copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), 0);
    EXPECT_EQ(copy3.getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy3.toString(),  std::string(std::string("MemoryModel {") +
                                             std::string(" .name = ") + std::string(modelName) +
                                             std::string(" .pageSize = ") + std::to_string(pageSize) +
                                             std::string(" .blockSize = ") + std::to_string(0) +
                                             std::string(" }")));

    EXPECT_EQ(copy3.toStringFull(),  std::string(std::string("MemoryModel {") +
                                                 std::string(" .name = ") + std::string(modelName) +
                                                 std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                 std::string(" .blockSize = ") + std::to_string(0) +
                                                 std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                 std::string(" .readTime = ") + std::to_string(readTime) +
                                                 std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                 std::string(" }")));


    delete model;
    delete copy;
}

GTEST_TEST(generalMemoryModelBasicTest, move)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_EQ(std::string(model->getModelName()), std::string(modelName));
    EXPECT_EQ(model->getPageSize(), pageSize);
    EXPECT_EQ(model->getBlockSize(), 0);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(model->readBytes(1), readTime);
    EXPECT_DOUBLE_EQ(model->writeBytes(1), writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1) * pageSize);

    MemoryModel* copy = new MemoryModelTest(std::move(*static_cast<MemoryModelTest*>(model)));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), 0);
    EXPECT_EQ(copy->getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy->toString(), std::string(std::string("MemoryModel {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(0) +
                                            std::string(" }")));

    EXPECT_EQ(copy->toStringFull(), std::string(std::string("MemoryModel {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(0) +
                                                std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                std::string(" .readTime = ") + std::to_string(readTime) +
                                                std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                std::string(" }")));

    MemoryModelTest copy2(std::move(*dynamic_cast<MemoryModelTest*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), 0);
    EXPECT_EQ(copy2.getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy2.toString(),  std::string(std::string("MemoryModel {") +
                                             std::string(" .name = ") + std::string(modelName) +
                                             std::string(" .pageSize = ") + std::to_string(pageSize) +
                                             std::string(" .blockSize = ") + std::to_string(0) +
                                             std::string(" }")));

    EXPECT_EQ(copy2.toStringFull(),  std::string(std::string("MemoryModel {") +
                                                 std::string(" .name = ") + std::string(modelName) +
                                                 std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                 std::string(" .blockSize = ") + std::to_string(0) +
                                                 std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                 std::string(" .readTime = ") + std::to_string(readTime) +
                                                 std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                 std::string(" }")));


    MemoryModelTest copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), 0);
    EXPECT_EQ(copy3.getMemoryWearOut(), (1) * pageSize);
    EXPECT_EQ(copy3.toString(),  std::string(std::string("MemoryModel {") +
                                             std::string(" .name = ") + std::string(modelName) +
                                             std::string(" .pageSize = ") + std::to_string(pageSize) +
                                             std::string(" .blockSize = ") + std::to_string(0) +
                                             std::string(" }")));

    EXPECT_EQ(copy3.toStringFull(),  std::string(std::string("MemoryModel {") +
                                                 std::string(" .name = ") + std::string(modelName) +
                                                 std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                 std::string(" .blockSize = ") + std::to_string(0) +
                                                 std::string(" .touchedBytes = ") + std::to_string(pageSize) +
                                                 std::string(" .readTime = ") + std::to_string(readTime) +
                                                 std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                 std::string(" }")));


    delete model;
    delete copy;
}

GTEST_TEST(generalMemoryModelBasicTest, read)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(model->readBytes(1), readTime);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize / 2), readTime);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize), readTime);
    EXPECT_DOUBLE_EQ(model->readBytes(pageSize + 1), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(model->readBytes(3 * pageSize), 3.0 * readTime);
    EXPECT_EQ(model->getMemoryWearOut(), 0);

    delete model;
}

GTEST_TEST(generalMemoryModelBasicTest, write)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(model->writeBytes(1), writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1) * pageSize);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize / 2), writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize), writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(model->writeBytes(pageSize + 1), 2.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1 + 2) * pageSize);

    EXPECT_DOUBLE_EQ(model->writeBytes(3 * pageSize), 3.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1 + 2 + 3) * pageSize);

    delete model;
}

GTEST_TEST(generalMemoryModelBasicTest, overwrite)
{
    const char* const modelName = "testModel";
    const size_t pageSize = 10;
    const double readTime = 0.1;
    const double writeTime = 4.4;

    MemoryModel* model = new MemoryModelTest(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(1), 100.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1) * pageSize);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize / 2), 100.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize), 100.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(pageSize + 1), 2.0 * 100.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1 + 2) * pageSize);

    EXPECT_DOUBLE_EQ(model->overwriteBytes(3 * pageSize), 3.0 * 100.0 * writeTime);
    EXPECT_EQ(model->getMemoryWearOut(), (1 + 1 + 1 + 2 + 3) * pageSize);

    delete model;
}