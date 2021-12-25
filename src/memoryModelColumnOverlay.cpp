#include <storage/memoryModelColumnOverlay.hpp>
#include <logger/logger.hpp>

MemoryModelColumnOverlay::MemoryModelColumnOverlay(MemoryModel* model)
: MemoryModel(*model)
{
    LOGGER_LOG_DEBUG("MemoryModelColumnOverlay created from model {}", toStringFull());
}

MemoryModelColumnOverlay::MemoryModelColumnOverlay(const char* modelName, size_t pageSize, size_t blockSize)
: MemoryModel(modelName, pageSize, blockSize)
{
    LOGGER_LOG_DEBUG("MemoryModelColumnOverlay created from scratch {}", toStringFull());
}

std::string MemoryModelColumnOverlay::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryModelColumnOverlay {") +
                           std::string(" .name = ") + std::string(modelName) +
                           std::string(" .pageSize = ") + std::to_string(pageSize) +
                           std::string(" .blockSize = ") + std::to_string(blockSize) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryModelColumnOverlay {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryModelColumnOverlay::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
            return std::string(std::string("MemoryModelColumnOverlay {") +
                               std::string(" .name = ") + std::string(modelName) +
                               std::string(" .pageSize = ") + std::to_string(pageSize) +
                               std::string(" .blockSize = ") + std::to_string(blockSize) +
                               std::string(" .touchedBytes = ") + std::to_string(touchedBytes) +
                               std::string(" }"));
    else
        return std::string(std::string("MemoryModelColumnOverlay {\n") +
                           std::string("\t.name = ") + std::string(modelName) + std::string("\n") +
                           std::string("\t.pageSize = ") + std::to_string(pageSize) + std::string("\n") +
                           std::string("\t.blockSize = ") + std::to_string(blockSize) + std::string("\n") +
                           std::string("\t.touchedBytes = ") + std::to_string(touchedBytes) + std::string("\n") +
                           std::string("}"));
}

void MemoryModelColumnOverlay::setWearOut(size_t bytes) noexcept(true)
{
    touchedBytes = bytes;
}

void MemoryModelColumnOverlay::resetWearOut() noexcept(true)
{
    touchedBytes = 0;
}

void MemoryModelColumnOverlay::addWearOut(size_t bytes) noexcept(true)
{
    touchedBytes += bytes;
}

void MemoryModelColumnOverlay::resetState() noexcept(true)
{
    // for now only reset wearout
    resetWearOut();
}

double MemoryModelColumnOverlay::writeBytes(size_t bytes) noexcept(true)
{
    (void)bytes;

    LOGGER_LOG_WARN("MemoryModelColumnOverlay should be used only to gather stats from severals columns");

    return 0.0;
}

double MemoryModelColumnOverlay::readBytes(size_t bytes) noexcept(true)
{
    (void)bytes;

    LOGGER_LOG_WARN("MemoryModelColumnOverlay should be used only to gather stats from severals columns");

    return 0.0;
}

double MemoryModelColumnOverlay::overwriteBytes(size_t bytes) noexcept(true)
{
    (void)bytes;

    LOGGER_LOG_WARN("MemoryModelColumnOverlay should be used only to gather stats from severals columns");

    return 0.0;
}