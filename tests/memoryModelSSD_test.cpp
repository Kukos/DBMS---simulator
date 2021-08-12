#include <storage/memoryModelSSD.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(ssdBasicTest, interface)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    EXPECT_EQ(std::string(ssd.getModelName()), std::string(modelName));
    EXPECT_EQ(ssd.getPageSize(), pageSize);
    EXPECT_EQ(ssd.getBlockSize(), blockSize);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_EQ(ssd.toString(), std::string(std::string("MemoryModelSSD {") +
                                          std::string(" .name = ") + std::string(modelName) +
                                          std::string(" .pageSize = ") + std::to_string(pageSize) +
                                          std::string(" .blockSize = ") + std::to_string(blockSize) +
                                          std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                          std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                          std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                          std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                          std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                          std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                          std::string(" }")));

    EXPECT_EQ(ssd.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                               std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                               std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                               std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                               std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                               std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                               std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                               std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                               std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                               std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                               std::string("}")));

    EXPECT_EQ(ssd.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                              std::string(" .name = ") + std::string(modelName) +
                                              std::string(" .pageSize = ") + std::to_string(pageSize) +
                                              std::string(" .blockSize = ") + std::to_string(blockSize) +
                                              std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                              std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                              std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                              std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                              std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                              std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                              std::string(" .dirtyPages = ") + std::to_string(0) +
                                              std::string(" }")));

    EXPECT_EQ(ssd.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                       std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                       std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                       std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                       std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                       std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                       std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                       std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                       std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                       std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                       std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                       std::string("}")));

    MemoryModel* samsung = new MemoryModelSSD_Samsung840();
    EXPECT_EQ(std::string(samsung->getModelName()), std::string("SSD:samsung840"));
    EXPECT_EQ(samsung->getPageSize(), 8192);
    EXPECT_EQ(samsung->getBlockSize(), 8192 * 64);
    EXPECT_EQ(samsung->getMemoryWearOut(), 0);
    delete samsung;

    MemoryModel* intel = new MemoryModelSSD_IntelDCP4511();
    EXPECT_EQ(std::string(intel->getModelName()), std::string("SSD:intelDCP4511"));
    EXPECT_EQ(intel->getPageSize(), 4096);
    EXPECT_EQ(intel->getBlockSize(), 4096 * 64);
    EXPECT_EQ(intel->getMemoryWearOut(), 0);
    delete intel;

    MemoryModel* toshiba = new MemoryModelSSD_ToshibaVX500();
    EXPECT_EQ(std::string(toshiba->getModelName()), std::string("SSD:toshibaVX500"));
    EXPECT_EQ(toshiba->getPageSize(), 4096);
    EXPECT_EQ(toshiba->getBlockSize(), 4096 * 64);
    EXPECT_EQ(toshiba->getMemoryWearOut(), 0);
    delete toshiba;
}

GTEST_TEST(ssdBasicTest, copy)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    EXPECT_EQ(std::string(ssd.getModelName()), std::string(modelName));
    EXPECT_EQ(ssd.getPageSize(), pageSize);
    EXPECT_EQ(ssd.getBlockSize(), blockSize);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_EQ(ssd.toString(), std::string(std::string("MemoryModelSSD {") +
                                          std::string(" .name = ") + std::string(modelName) +
                                          std::string(" .pageSize = ") + std::to_string(pageSize) +
                                          std::string(" .blockSize = ") + std::to_string(blockSize) +
                                          std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                          std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                          std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                          std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                          std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                          std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                          std::string(" }")));

    EXPECT_EQ(ssd.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                               std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                               std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                               std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                               std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                               std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                               std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                               std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                               std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                               std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                               std::string("}")));

    EXPECT_EQ(ssd.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                              std::string(" .name = ") + std::string(modelName) +
                                              std::string(" .pageSize = ") + std::to_string(pageSize) +
                                              std::string(" .blockSize = ") + std::to_string(blockSize) +
                                              std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                              std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                              std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                              std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                              std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                              std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                              std::string(" .dirtyPages = ") + std::to_string(0) +
                                              std::string(" }")));

    EXPECT_EQ(ssd.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                       std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                       std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                       std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                       std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                       std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                       std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                       std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                       std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                       std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                       std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                       std::string("}")));

    MemoryModel* copy = new MemoryModelSSD(ssd);

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    EXPECT_EQ(copy->toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy->toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy->toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy->toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));

    MemoryModelSSD copy2(*dynamic_cast<MemoryModelSSD*>(copy));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy2.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy2.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy2.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));


    MemoryModelSSD copy3;
    copy3 = copy2;
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    EXPECT_EQ(copy3.toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy3.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy3.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy3.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));


    delete copy;
}

GTEST_TEST(ssdBasicTest, move)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);
    EXPECT_EQ(std::string(ssd.getModelName()), std::string(modelName));
    EXPECT_EQ(ssd.getPageSize(), pageSize);
    EXPECT_EQ(ssd.getBlockSize(), blockSize);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_EQ(ssd.toString(), std::string(std::string("MemoryModelSSD {") +
                                          std::string(" .name = ") + std::string(modelName) +
                                          std::string(" .pageSize = ") + std::to_string(pageSize) +
                                          std::string(" .blockSize = ") + std::to_string(blockSize) +
                                          std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                          std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                          std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                          std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                          std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                          std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                          std::string(" }")));

    EXPECT_EQ(ssd.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                               std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                               std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                               std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                               std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                               std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                               std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                               std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                               std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                               std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                               std::string("}")));

    EXPECT_EQ(ssd.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                              std::string(" .name = ") + std::string(modelName) +
                                              std::string(" .pageSize = ") + std::to_string(pageSize) +
                                              std::string(" .blockSize = ") + std::to_string(blockSize) +
                                              std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                              std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                              std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                              std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                              std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                              std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                              std::string(" .dirtyPages = ") + std::to_string(0) +
                                              std::string(" }")));

    EXPECT_EQ(ssd.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                       std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                       std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                       std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                       std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                       std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                       std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                       std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                       std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                       std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                       std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                       std::string("}")));

    MemoryModel* copy = new MemoryModelSSD(std::move(ssd));

    EXPECT_EQ(std::string(copy->getModelName()), std::string(modelName));
    EXPECT_EQ(copy->getPageSize(), pageSize);
    EXPECT_EQ(copy->getBlockSize(), blockSize);
    EXPECT_EQ(copy->getMemoryWearOut(), 0);

    EXPECT_EQ(copy->toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy->toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy->toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy->toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));

    MemoryModelSSD copy2(std::move(*dynamic_cast<MemoryModelSSD*>(copy)));

    EXPECT_EQ(std::string(copy2.getModelName()), std::string(modelName));
    EXPECT_EQ(copy2.getPageSize(), pageSize);
    EXPECT_EQ(copy2.getBlockSize(), blockSize);
    EXPECT_EQ(copy2.getMemoryWearOut(), 0);

    EXPECT_EQ(copy2.toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy2.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy2.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy2.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));


    MemoryModelSSD copy3;
    copy3 = std::move(copy2);
    EXPECT_EQ(std::string(copy3.getModelName()), std::string(modelName));
    EXPECT_EQ(copy3.getPageSize(), pageSize);
    EXPECT_EQ(copy3.getBlockSize(), blockSize);
    EXPECT_EQ(copy3.getMemoryWearOut(), 0);

    EXPECT_EQ(copy3.toString(), std::string(std::string("MemoryModelSSD {") +
                                            std::string(" .name = ") + std::string(modelName) +
                                            std::string(" .pageSize = ") + std::to_string(pageSize) +
                                            std::string(" .blockSize = ") + std::to_string(blockSize) +
                                            std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                            std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                            std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                            std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                            std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                            std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                            std::string(" }")));

    EXPECT_EQ(copy3.toString(false), std::string(std::string("MemoryModelSSD {\n") +
                                                 std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                 std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                 std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                 std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                 std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                 std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                 std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                 std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                 std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                 std::string("}")));

    EXPECT_EQ(copy3.toStringFull(), std::string(std::string("MemoryModelSSD {") +
                                                std::string(" .name = ") + std::string(modelName) +
                                                std::string(" .pageSize = ") + std::to_string(pageSize) +
                                                std::string(" .blockSize = ") + std::to_string(blockSize) +
                                                std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                                                std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                                                std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                                                std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                                                std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                                                std::string(" .seqOpTreshold = ") + std::to_string(4) +
                                                std::string(" .dirtyPages = ") + std::to_string(0) +
                                                std::string(" }")));

    EXPECT_EQ(copy3.toStringFull(false), std::string(std::string("MemoryModelSSD {\n") +
                                                     std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                                                     std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                                                     std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                                                     std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                                                     std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                                                     std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                                                     std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                                                     std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                                                     std::string("\t.seqOpTreshold = ") + std::to_string(4) + std::string("\n") +
                                                     std::string("\t.dirtyPages = ") + std::to_string(0) + std::string("\n") +
                                                     std::string("}")));


    delete copy;
}

GTEST_TEST(ssdBasicTest, readRandom)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);

    EXPECT_DOUBLE_EQ(ssd.readBytes(0), 0.0);
    EXPECT_DOUBLE_EQ(ssd.readBytes(1), readRandomTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(pageSize / 2), readRandomTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(pageSize), readRandomTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(pageSize + 1), 2.0 * readRandomTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(3 * pageSize), 3.0 * readRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);
}

GTEST_TEST(ssdBasicTest, writeRandom)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(0), 0.0);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(1), writeRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (1) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(pageSize / 2), writeRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(pageSize), writeRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (1 + 1 + 1) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(pageSize + 1), 2.0 * writeRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (1 + 1 + 1 + 2) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(3 * pageSize), 3.0 * writeRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (1 + 1 + 1 + 2 + 3) * pageSize);
}

GTEST_TEST(ssdBasicTest, readSeq)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);

    EXPECT_DOUBLE_EQ(ssd.readBytes(0), 0.0);
    EXPECT_DOUBLE_EQ(ssd.readBytes(4 * pageSize), 4.0 * readSeqTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(4 * pageSize + pageSize / 2),  5.0 * readSeqTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(5 * pageSize + 1),  6.0 * readSeqTime);
    EXPECT_DOUBLE_EQ(ssd.readBytes(100 * pageSize),  100.0 * readSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);
}

GTEST_TEST(ssdBasicTest, writeSeq)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(0), 0.0);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(4 * pageSize), 4.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(4 * pageSize + pageSize / 2),  5.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 5) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(5 * pageSize + 1),  6.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 5 + 6) * pageSize);

    EXPECT_DOUBLE_EQ(ssd.writeBytes(100 * pageSize),  100.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 5 + 6 + 100) * pageSize);
}

GTEST_TEST(ssdBasicTest, erase)
{
    const char* const modelName = "ssd";
    const size_t pageSize = 2048;
    const size_t blockSize = pageSize * 32;
    const double readRandomTime = 1.0;
    const double writeRandomTime = 2.0;
    const double readSeqTime = 0.1;
    const double writeSeqTime = 0.2;
    const double eraseTime = 10.0;

    MemoryModelSSD ssd(modelName, pageSize, blockSize, readRandomTime, writeRandomTime, readSeqTime, writeSeqTime, eraseTime);

    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(0), 0.0);
    EXPECT_EQ(ssd.getMemoryWearOut(), 0);

    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(4 * pageSize), 4.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4) * pageSize);

    // 4 pages are dirty. 28 clean left
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(27 * pageSize + 1), 28.0 * writeSeqTime + eraseTime + readRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28) * pageSize);

    // all are clean now, lets make 10 pages dirty
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(10 * pageSize), 10.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10) * pageSize);

    // Lets make another 10 as dirty
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(9 * pageSize + 100), 10.0 * writeSeqTime + readRandomTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10) * pageSize);

    // Lets make another 100 pages as dirty. In total we have 20 + 100 dirty. 120 / 32 = 3x erase + 24 dirty pages
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(100 * pageSize), 100.0 * writeSeqTime + 3.0 * eraseTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10 + 100) * pageSize);

    // Make 7 pages dirty
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(7 * pageSize), 7.0 * writeSeqTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10 + 100 + 7) * pageSize);

    // Last clean page. Make page as dirty
    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(1 * pageSize), writeRandomTime + eraseTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10 + 100 + 7 + 1) * pageSize);

    // Lets test if randomWrite can call erase
    for (size_t i = 0; i < blockSize / pageSize - 2; ++i)
    {
        EXPECT_DOUBLE_EQ(ssd.overwriteBytes(1 * pageSize), writeRandomTime);
        EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10 + 100 + 7 + 1 + (i + 1)) * pageSize);
    }

    EXPECT_DOUBLE_EQ(ssd.overwriteBytes(2 * pageSize), 2.0 * writeRandomTime + eraseTime);
    EXPECT_EQ(ssd.getMemoryWearOut(), (4 + 28 + 10 + 10 + 100 + 7 + 1 + (blockSize / pageSize - 2) + 2) * pageSize);
}
