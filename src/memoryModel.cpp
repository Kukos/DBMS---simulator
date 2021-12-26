#include <storage/memoryModel.hpp>
#include <logger/logger.hpp>

size_t MemoryModel::bytesToPages(size_t bytes) const noexcept(true)
{
    return (bytes + (pageSize - 1)) / pageSize;
}

size_t MemoryModel::bytesToBlocks(size_t bytes) const noexcept(true)
{
    return (bytes + (blockSize - 1)) / blockSize;
}

std::string MemoryModel::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModel {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModel {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModel::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
            return std::string(std::string("MemoryModel {") +
                               std::string(" .name = ") + std::string(modelName) +
                               std::string(" .pageSize = ") + std::to_string(pageSize) +
                               std::string(" .blockSize = ") + std::to_string(blockSize) +
                               std::string(" .touchedBytes = ") + std::to_string(touchedBytes) +
                               std::string(" }"));
    else
        return std::string(std::string("MemoryModel {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.touchedBytes = ") + std::to_string(touchedBytes) + std::string("\n") +
                           std::string("}"));
}

void MemoryModel::resetState() noexcept(true)
{
    // for now only reset wearout
    touchedBytes = 0;
}

MemoryModel::MemoryModel(const char* modelName, size_t pageSize, size_t blockSize)
: modelName{modelName}, pageSize{pageSize}, blockSize{blockSize}, touchedBytes{0}
{
    LOGGER_LOG_DEBUG("Memory model created: {}", toStringFull());
}

