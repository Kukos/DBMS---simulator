#include <storage/memoryModelPCM.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>


GTEST_TEST(pcmBasicTest, interface)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);
    EXPECT_EQ(std::string(pcm.getModelName()), std::string(modelName));
    EXPECT_EQ(pcm.getPageSize(), pageSize);
    EXPECT_EQ(pcm.getBlockSize(), blockSize);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);

    EXPECT_EQ(pcm.toString(), std::string(std::string("MemoryModelPCM {") +
                                          std::string(" .name = ") + std::string(modelName) +
                                          std::string(" .pageSize = ") + std::to_string(pageSize) +
                                          std::string(" .blockSize = ") + std::to_string(blockSize) +
                                          std::string(" .readTime = ") + std::to_string(readTime) +
                                          std::string(" .writeTime = ") + std::to_string(writeTime) +
                                          std::string(" }")));

    EXPECT_EQ(pcm.toString(false), std::string(std::string("MemoryModelPCM {\n") +
                                               std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                               std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                               std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                               std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                                               std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                                               std::string("}")));

    EXPECT_EQ(pcm.toStringFull(), std::string(std::string("MemoryModelPCM {") +
                                              std::string(" .name = ") + std::string(modelName) +
                                              std::string(" .pageSize = ") + std::to_string(pageSize) +
                                              std::string(" .blockSize = ") + std::to_string(blockSize) +
                                              std::string(" .readTime = ") + std::to_string(readTime) +
                                              std::string(" .writeTime = ") + std::to_string(writeTime) +
                                              std::string(" }")));

    EXPECT_EQ(pcm.toStringFull(false), std::string(std::string("MemoryModelPCM {\n") +
                                                   std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                   std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                   std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                   std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                                                   std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                                                   std::string("}")));

    MemoryModel* pcmDefault = new MemoryModelPCM_DefaultModel();
    EXPECT_EQ(std::string(pcmDefault->getModelName()), std::string("PCM:defaultModel"));
    EXPECT_EQ(pcmDefault->getPageSize(), 64);
    EXPECT_EQ(pcmDefault->getBlockSize(), 0);
    EXPECT_EQ(pcmDefault->getMemoryWearOut(), 0);

    delete pcmDefault;
}

GTEST_TEST(pcmBasicTest, copy)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);
    EXPECT_EQ(std::string(pcm.getModelName()), std::string(modelName));
    EXPECT_EQ(pcm.getPageSize(), pageSize);
    EXPECT_EQ(pcm.getBlockSize(), blockSize);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);

    MemoryModel* copy = new MemoryModelPCM(pcm);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    MemoryModelPCM copy2(*dynamic_cast<MemoryModelPCM*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    MemoryModelPCM copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    delete copy;
}

GTEST_TEST(pcmBasicTest, move)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const size_t blockSize = 0;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);
    EXPECT_EQ(std::string(pcm.getModelName()), std::string(modelName));
    EXPECT_EQ(pcm.getPageSize(), pageSize);
    EXPECT_EQ(pcm.getBlockSize(), blockSize);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);

    MemoryModel* copy = new MemoryModelPCM(std::move(pcm));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    MemoryModelPCM copy2(std::move(*dynamic_cast<MemoryModelPCM*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    MemoryModelPCM copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    delete copy;
}

GTEST_TEST(pcmBasicTest, read)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(pcm.readBytes(0), 0.0);
    EXPECT_DOUBLE_EQ(pcm.readBytes(1), readTime);
    EXPECT_DOUBLE_EQ(pcm.readBytes(pageSize / 2), readTime);
    EXPECT_DOUBLE_EQ(pcm.readBytes(pageSize), readTime);
    EXPECT_DOUBLE_EQ(pcm.readBytes(pageSize + 1), 2.0 * readTime);
    EXPECT_DOUBLE_EQ(pcm.readBytes(3 * pageSize), 3.0 * readTime);
    EXPECT_DOUBLE_EQ(pcm.readBytes(100), 13 * readTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);
}

GTEST_TEST(pcmBasicTest, write)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(0), 0.0);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(1), writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(pageSize / 2), writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(pageSize), writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(pageSize + 1), 2.0 * writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(3 * pageSize), 3.0 * writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1 + 3 * pageSize);

    EXPECT_DOUBLE_EQ(pcm.writeBytes(100), 13 * writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1 + 3 * pageSize + 100);
}

GTEST_TEST(pcmBasicTest, overwrite)
{
    const char* const modelName = "pcm";
    const size_t pageSize = 8;
    const double readTime = 0.1;
    const double writeTime = 2.0;

    MemoryModelPCM pcm(modelName, pageSize, readTime, writeTime);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(0), 0.0);
    EXPECT_EQ(pcm.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(1), writeTime + readTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(pageSize / 2), writeTime + readTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(pageSize), writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(pageSize + 1), 2.0 * writeTime + readTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(3 * pageSize), 3.0 * writeTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1 + 3 * pageSize);

    EXPECT_DOUBLE_EQ(pcm.overwriteBytes(100), 13 * writeTime + readTime);
    EXPECT_EQ(pcm.getMemoryWearOut(), 1 + pageSize / 2 + pageSize + pageSize + 1 + 3 * pageSize + 100);
}

