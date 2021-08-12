#include <storage/memoryModelFlashNandRaw.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(flashNandRawBasicTest, interface)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    EXPECT_EQ(std::string(flash.getModelName()), std::string(modelName));
    EXPECT_EQ(flash.getPageSize(), pageSize);
    EXPECT_EQ(flash.getBlockSize(), blockSize);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);

    EXPECT_EQ(flash.toString(), std::string(std::string("MemoryModelFlashRaw {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readTime = ") + std::to_string(readTime) +
                                            std::string(" .writeTime = ") + std::to_string(writeTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" }")));

    EXPECT_EQ(flash.toString(false), std::string(std::string("MemoryModelFlashRaw {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                                                 std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(flash.toStringFull(), std::string(std::string("MemoryModelFlashRaw {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readTime = ") + std::to_string(readTime) +
                                                std::string(" .writeTime = ") + std::to_string(writeTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" }")));

    EXPECT_EQ(flash.toStringFull(false), std::string(std::string("MemoryModelFlashRaw {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                                                     std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("}")));

    MemoryModel* samsung = new MemoryModelFlashNandRaw_SamsungK9F1G08U0D();
    EXPECT_EQ(std::string(samsung->getModelName()), std::string("FlashNandRaw:samsungK9F1G08U0D"));
    EXPECT_EQ(samsung->getPageSize(), 2048);
    EXPECT_EQ(samsung->getBlockSize(), 2048 * 32);
    EXPECT_EQ(samsung->getMemoryWearOut(), 0);
    delete samsung;

    MemoryModel* micron1 = new MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA();
    EXPECT_EQ(std::string(micron1->getModelName()), std::string("FlashNandRaw:micronMT29F32G08ABAAA"));
    EXPECT_EQ(micron1->getPageSize(), 8192);
    EXPECT_EQ(micron1->getBlockSize(), 8192 * 128);
    EXPECT_EQ(micron1->getMemoryWearOut(), 0);
    delete micron1;

    MemoryModel* micron2 = new MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1();
    EXPECT_EQ(std::string(micron2->getModelName()), std::string("FlashNandRaw:micronMT29F32G08CBEDBL83A3WC1"));
    EXPECT_EQ(micron2->getPageSize(), 4096);
    EXPECT_EQ(micron2->getBlockSize(), 4096 * 128);
    EXPECT_EQ(micron2->getMemoryWearOut(), 0);
    delete micron2;
}

GTEST_TEST(flashNandRawBasicTest, copy)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    EXPECT_EQ(std::string(flash.getModelName()), std::string(modelName));
    EXPECT_EQ(flash.getPageSize(), pageSize);
    EXPECT_EQ(flash.getBlockSize(), blockSize);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);

    MemoryModel* copy = new MemoryModelFlashNandRaw(flash);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    MemoryModelFlashNandRaw copy2(*dynamic_cast<MemoryModelFlashNandRaw*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    MemoryModelFlashNandRaw copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    delete copy;
}

GTEST_TEST(flashNandRawBasicTest, move)
{
    const char* const modelName = "flashNandFTL";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);
    EXPECT_EQ(std::string(flash.getModelName()), std::string(modelName));
    EXPECT_EQ(flash.getPageSize(), pageSize);
    EXPECT_EQ(flash.getBlockSize(), blockSize);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);

    MemoryModel* copy = new MemoryModelFlashNandRaw(std::move(flash));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    MemoryModelFlashNandRaw copy2(std::move(*dynamic_cast<MemoryModelFlashNandRaw*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    MemoryModelFlashNandRaw copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    delete copy;
}

GTEST_TEST(flashNandRawBasicTest, read)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);

    EXPECT_DOUBLE_EQ(flash.readBytes(0), 0.0);
    EXPECT_DOUBLE_EQ(flash.readBytes(1), readTime);
    EXPECT_DOUBLE_EQ(flash.readBytes(pageSize / 2), readTime);
    EXPECT_DOUBLE_EQ(flash.readBytes(pageSize), readTime);
    EXPECT_DOUBLE_EQ(flash.readBytes(pageSize + 1), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(flash.readBytes(3 * pageSize), 3.0 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);
}

GTEST_TEST(flashNandRawBasicTest, write)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);

    EXPECT_DOUBLE_EQ(flash.writeBytes(0), 0.0);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(flash.writeBytes(1), writeTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (1) * pageSize);

    EXPECT_DOUBLE_EQ(flash.writeBytes(pageSize / 2), writeTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(flash.writeBytes(pageSize), writeTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (1 + 1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(flash.writeBytes(pageSize + 1), 2.0 * writeTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (1 + 1 + 1 + 2) * pageSize);

    EXPECT_DOUBLE_EQ(flash.writeBytes(3 * pageSize), 3.0 * writeTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (1 + 1 + 1 + 2 + 3) * pageSize);
}

GTEST_TEST(flashNandRawBasicTest, erase)
{
    const char* const modelName = "flashNandRaw";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readTime = 0.4;
    const double writeTime = 1.5;
    const double eraseTime = 5.0;

    MemoryModelFlashNandRaw flash(modelName, pageSize, blockSize, readTime, writeTime, eraseTime);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(0), 0.0);
    EXPECT_EQ(flash.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(4 * pageSize), 32.0 * writeTime + eraseTime + 28.0 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (32) * pageSize);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(27 * pageSize + 1), 32.0 * writeTime + eraseTime + 5.0 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (32 + 32) * pageSize);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(10 * pageSize), 32.0 * writeTime + eraseTime + 22.0 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (32 + 32 + 32) * pageSize);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(9 * pageSize + 100), 32.0 * writeTime + eraseTime + 23.0 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (32 + 32 + 32 + 32) * pageSize);

    EXPECT_DOUBLE_EQ(flash.overwriteBytes(100 * pageSize), 128.0 * writeTime + 4.0 * eraseTime + 28 * readTime);
    EXPECT_EQ(flash.getMemoryWearOut(), (32 + 32 + 32 + 32 + 128) * pageSize);
}