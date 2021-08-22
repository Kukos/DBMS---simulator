#include <storage/memoryModelPCM.hpp>
#include <logger/logger.hpp>

double MemoryModelPCM::readMemLines(size_t memLines) const noexcept(true)
{
    const double time = static_cast<double>(memLines) * readTime;

    LOGGER_LOG_TRACE("reading memLines {}, took time {}", memLines, time);
    return time;
}

double MemoryModelPCM::writeMemLines(size_t memLines) noexcept(true)
{
    const double time = static_cast<double>(memLines) * writeTime;

    LOGGER_LOG_TRACE("writting memLines {}, took time {}", memLines, time);
    return time;
}

MemoryModelPCM::MemoryModelPCM(const char* modelName,
                               size_t memLine,
                               double readTime,
                               double writeTime)
: MemoryModel(modelName, memLine, 0), readTime{readTime}, writeTime{writeTime}
{
    LOGGER_LOG_DEBUG("PCM Memory model created: {}", toStringFull());
}

double MemoryModelPCM::writeBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    /*
        PCM is byte addresssed memory, so we can write even 8B,
        but PCM has several dies, so in write CYCLE we can write full memLine
    */
    const size_t memLines = bytesToPages(bytes);

    // PCM is byte addressed, so we can touched only specific bytes
    touchedBytes += bytes;

    return writeMemLines(memLines);
}

double MemoryModelPCM::overwriteBytes(size_t bytes) noexcept(true)
{
    double time = 0.0;

    if (bytes == 0)
        return 0.0;

    // read bytes to rewrite
    if (bytes % pageSize != 0)
        time += readBytes(pageSize - bytes % pageSize);

    // write new bytes + rewrite existing bytes from page
    time += writeBytes(bytes);

    return time;
}

double MemoryModelPCM::readBytes(size_t bytes) noexcept(true)
{
    if (bytes == 0.0)
        return 0.0;

    /*
        PCM is byte addresssed memory, so we can read even 8B,
        but PCM has several dies, so in read CYCLE we can read full memLine
    */
    const size_t memLines = bytesToPages(bytes);

    return readMemLines(memLines);
}

std::string MemoryModelPCM::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelPCM {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelPCM {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModelPCM::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelPCM {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" .readTime = ") + std::to_string(readTime) +
                           std::string(" .writeTime = ") + std::to_string(writeTime) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelPCM {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.readTime = ") + std::to_string(readTime) + std::string("\n") +
                           std::string("\t.writeTime = ") + std::to_string(writeTime) + std::string("\n") +
                           std::string("}"));
}