#include <storage/memoryModelSSD.hpp>
#include <logger/logger.hpp>

double MemoryModelSSD::readPagesRandom(size_t pages) const noexcept(true)
{
    const double time = static_cast<double>(pages) * readRandomTime;

    LOGGER_LOG_TRACE("reading pages(random) {}, took time {}", pages, time);
    return time;
}

double MemoryModelSSD::writePagesRandom(size_t pages) noexcept(true)
{
    const double time = static_cast<double>(pages) * writeRandomTime;

    touchedBytes += pages * pageSize;

    LOGGER_LOG_TRACE("writing pages(random) {}, took time {}", pages, time);
    return time;
}

double MemoryModelSSD::readPagesSeq(size_t pages) const noexcept(true)
{
    const double time = static_cast<double>(pages) * readSeqTime;

    LOGGER_LOG_TRACE("reading pages(seq) {}, took time {}", pages, time);
    return time;
}

double MemoryModelSSD::writePagesSeq(size_t pages) noexcept(true)
{
    const double time = static_cast<double>(pages) * writeSeqTime;

    touchedBytes += pages * pageSize;

    LOGGER_LOG_TRACE("writing pages(seq) {}, took time {}", pages, time);
    return time;
}

double MemoryModelSSD::flushDirtyPages() noexcept(true)
{
    const size_t blocksToErase = dirtyPages / pagesInBlock;

    // We can wait now
    if (blocksToErase == 0)
        return 0.0;

    return eraseBlocks(blocksToErase);
}

double MemoryModelSSD::eraseBlocks(size_t blocks) noexcept(true)
{
    const double time = static_cast<double>(blocks) * eraseTime;
    dirtyPages -= pagesInBlock * blocks;

    LOGGER_LOG_TRACE("erasing blocks {}, took time {}", blocks, time);
    return time;
}

MemoryModelSSD::MemoryModelSSD(const char* modelName,
                               size_t pageSize,
                               size_t blockSize,
                               double readRandomTime,
                               double writeRandomTime,
                               double readSeqTime,
                               double writeSeqTime,
                               double eraseTime)
: MemoryModel(modelName, pageSize, blockSize), pagesInBlock{blockSize / pageSize}, readRandomTime{readRandomTime}, writeRandomTime{writeRandomTime}, readSeqTime{readSeqTime}, writeSeqTime{writeSeqTime}, eraseTime{eraseTime}, dirtyPages{0}
{
    LOGGER_LOG_DEBUG("SSD Memory model created: {}", toStringFull());
}

double MemoryModelSSD::writeBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t pages = bytesToPages(bytes);

    return pages < seqOpTreshold ? writePagesRandom(pages) : writePagesSeq(pages);
}

double MemoryModelSSD::overwriteBytes(size_t bytes) noexcept(true)
{
    double time = 0.0;

    if (bytes == 0)
        return 0.0;

    /* read bytes to rewrite */
    if (bytes % pageSize != 0)
        time += readBytes(pageSize - bytes % pageSize);

    /* write new bytes + rewrite existing bytes from page */
    time += writeBytes(bytes);

    const size_t pages = bytesToPages(bytes);
    dirtyPages += pages;

    time += flushDirtyPages();

    return time;
}

double MemoryModelSSD::readBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t pages = bytesToPages(bytes);

    return pages < seqOpTreshold ? readPagesRandom(pages) : readPagesSeq(pages);
}

std::string MemoryModelSSD::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelSSD {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                           std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                           std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                           std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" .seqOpTreshold = ") + std::to_string(seqOpTreshold) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelSSD {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                           std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                           std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                           std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("\t.seqOpTreshold = ") + std::to_string(seqOpTreshold) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModelSSD::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelSSD {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readRandomTime = ") + std::to_string(readRandomTime) +
                           std::string(" .writeRandomTime = ") + std::to_string(writeRandomTime) +
                           std::string(" .readSeqTime = ") + std::to_string(readSeqTime) +
                           std::string(" .writeSeqTime = ") + std::to_string(writeSeqTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" .seqOpTreshold = ") + std::to_string(seqOpTreshold) +
                           std::string(" .dirtyPages = ") + std::to_string(dirtyPages) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelSSD {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readRandomTime = ") + std::to_string(readRandomTime) + std::string("\n") +
                           std::string("\t.writeRandomTime = ") + std::to_string(writeRandomTime) + std::string("\n") +
                           std::string("\t.readSeqTime = ") + std::to_string(readSeqTime) + std::string("\n") +
                           std::string("\t.writeSeqTime = ") + std::to_string(writeSeqTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("\t.seqOpTreshold = ") + std::to_string(seqOpTreshold) + std::string("\n") +
                           std::string("\t.dirtyPages = ") + std::to_string(dirtyPages) + std::string("\n") +
                           std::string("}"));
}