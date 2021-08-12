#include <storage/memoryModelFlashNandFTL.hpp>
#include <logger/logger.hpp>

double MemoryModelFlashNandFTL::readPages(size_t pages) const noexcept(true)
{
    const double time = static_cast<double>(pages) * readTime;

    LOGGER_LOG_TRACE("reading pages {}, took time {}", pages, time);
    return time;
}

double MemoryModelFlashNandFTL::writePages(size_t pages) noexcept(true)
{
    const double time = static_cast<double>(pages) * writeTime;

    touchedBytes += pages * pageSize;

    LOGGER_LOG_TRACE("writing pages {}, took time {}", pages, time);
    return time;
}

double MemoryModelFlashNandFTL::flushDirtyPages() noexcept(true)
{
    const size_t blocksToErase = dirtyPages / pagesInBlock;

    // We can wait now
    if (blocksToErase == 0)
        return 0.0;

    return eraseBlocks(blocksToErase);
}

double MemoryModelFlashNandFTL::eraseBlocks(size_t blocks) noexcept(true)
{
    const double time = static_cast<double>(blocks) * eraseTime;
    dirtyPages -= pagesInBlock * blocks;

    LOGGER_LOG_TRACE("erasing blocks {}, took time {}", blocks, time);
    return time;
}

MemoryModelFlashNandFTL::MemoryModelFlashNandFTL(const char* modelName,
                                                 size_t pageSize,
                                                 size_t blockSize,
                                                 double readTime,
                                                 double writeTime,
                                                 double eraseTime)
: MemoryModel(modelName, pageSize, blockSize), pagesInBlock{blockSize / pageSize}, readTime{readTime}, writeTime{writeTime}, eraseTime{eraseTime}, dirtyPages{0}
{
    LOGGER_LOG_DEBUG("Flash FTL Memory model created: {}", toStringFull());
}

double MemoryModelFlashNandFTL::writeBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t pages = bytesToPages(bytes);

    return writePages(pages);
}

double MemoryModelFlashNandFTL::overwriteBytes(size_t bytes) noexcept(true)
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

double MemoryModelFlashNandFTL::readBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t pages = bytesToPages(bytes);

    return readPages(pages);
}

std::string MemoryModelFlashNandFTL::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelFlashFTL {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelFlashFTL {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModelFlashNandFTL::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelFlashFTL {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" .dirtyPages = ") + std::to_string(dirtyPages) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelFlashFTL {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("\t.dirtyPages = ") + std::to_string(dirtyPages) + std::string("\n") +
                           std::string("}"));
}