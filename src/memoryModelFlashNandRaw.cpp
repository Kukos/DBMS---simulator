#include <storage/memoryModelFlashNandRaw.hpp>
#include <logger/logger.hpp>

double MemoryModelFlashNandRaw::readPages(size_t pages) const noexcept(true)
{
    const double time = static_cast<double>(pages) * readTime;

    LOGGER_LOG_TRACE("reading pages {}, took time {}", pages, time);
    return time;
}

double MemoryModelFlashNandRaw::writePages(size_t pages) noexcept(true)
{
    const double time = static_cast<double>(pages) * writeTime;

    touchedBytes += pages * pageSize;

    LOGGER_LOG_TRACE("writing pages {}, took time {}", pages, time);
    return time;
}

double MemoryModelFlashNandRaw::eraseBlocks(size_t blocks) noexcept(true)
{
    const double time = static_cast<double>(blocks) * eraseTime;

    LOGGER_LOG_TRACE("erasing blocks {}, took time {}", blocks, time);
    return time;
}

MemoryModelFlashNandRaw::MemoryModelFlashNandRaw(const char* modelName,
                                                 size_t pageSize,
                                                 size_t blockSize,
                                                 double readTime,
                                                 double writeTime,
                                                 double eraseTime)
: MemoryModel(modelName, pageSize, blockSize), readTime{readTime}, writeTime{writeTime}, eraseTime{eraseTime}
{
    LOGGER_LOG_DEBUG("Flash Raw Memory model created: {}", toStringFull());
}

double MemoryModelFlashNandRaw::writeBytes(size_t bytes) noexcept(true)
{
    const size_t pages = bytesToPages(bytes);

    return writePages(pages);
}

double MemoryModelFlashNandRaw::overwriteBytes(size_t bytes) noexcept(true)
{
    double time = 0.0;

    /* read bytes to rewrite */
    if (bytes % blockSize != 0)
        time += readBytes(blockSize - bytes % blockSize);

    /* erase blocks */
    const size_t blocks = bytesToBlocks(bytes);
    time += eraseBlocks(blocks);

    /* write new bytes and bytes from block, so we need to write full blocks */
    time += writeBytes(blocks * blockSize);

    return time;
}

double MemoryModelFlashNandRaw::readBytes(size_t bytes) noexcept(true)
{
    const size_t pages = bytesToPages(bytes);

    return readPages(pages);
}

std::string MemoryModelFlashNandRaw::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelFlashRaw {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelFlashRaw {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModelFlashNandRaw::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelFlashRaw {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" .eraseTime = ") + std::to_string(eraseTime) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelFlashRaw {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("\t.eraseTime = ") + std::to_string(eraseTime) + std::string("\n") +
                           std::string("}"));
}