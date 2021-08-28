#include <storage/memoryControllerSSD.hpp>
#include <logger/logger.hpp>

#include <numeric>

MemoryControllerSSD::MemoryControllerSSD(MemoryModelSSD* ssd)
: MemoryController(ssd)
{
    LOGGER_LOG_DEBUG("Memory controller SSD created: {}", toStringFull());
}

std::string MemoryControllerSSD::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryControllerSSD {") +
                           std::string(" .memoryModel = ") + memoryModel->toString() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryControllerSSD {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toString()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryControllerSSD::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromPageQueue = [](const std::string &accumulator, const size_t &page)
    {
        return accumulator.empty() ? std::to_string(page) : accumulator + "," + std::to_string(page);
    };

    auto buildStringFromOverWriteQueue = [](const std::string &accumulator, const std::pair<size_t, std::vector<bool>> &pair)
    {
        return accumulator.empty() ? std::to_string(pair.first) : accumulator + "," + std::to_string(pair.first);
    };

    const std::string writeQueueString =  std::string("{") +
                                          std::accumulate(std::begin(writeCache), std::end(writeCache), std::string(), buildStringFromPageQueue) +
                                          std::string("}");

    const std::string readQueueString =   std::string("{") +
                                          std::accumulate(std::begin(readCache), std::end(readCache), std::string(), buildStringFromPageQueue) +
                                          std::string("}");

    const std::string overwriteQueueStringDebug = std::string("{") +
                                                  std::accumulate(std::begin(overwriteCache), std::end(overwriteCache), std::string(), buildStringFromOverWriteQueue) +
                                                  std::string("}");

    if (oneLine)
        return std::string(std::string("MemoryControllerSSD {") +
                           std::string(" .memoryModel = ") + memoryModel->toStringFull() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" .readCache = ") + writeQueueString +
                           std::string(" .writeCache = ") + readQueueString +
                           std::string(" .overwriteCache = ") + overwriteQueueStringDebug +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryControllerSSD {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toStringFull()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("\t.readCache = ") + writeQueueString + std::string("\n") +
                           std::string("\t.writeCache = ") + readQueueString + std::string("\n") +
                           std::string("\t.overwriteCache = ") + overwriteQueueStringDebug + std::string("\n") +
                           std::string("}"));
}