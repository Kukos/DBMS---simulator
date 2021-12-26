#include <storage/memoryControllerColumnOverlay.hpp>
#include <storage/memoryModelColumnOverlay.hpp>

#include <numeric>

MemoryControllerColumnOverlay::MemoryControllerColumnOverlay(MemoryModel* model)
: MemoryController(new MemoryModelColumnOverlay(model))
{
    LOGGER_LOG_DEBUG("MemoryControllerColumnOverlay created from model: {}", toStringFull());
}

MemoryControllerColumnOverlay::MemoryControllerColumnOverlay(MemoryController* controller)
: MemoryController(*controller)
{
    memoryModel.reset(new MemoryModelColumnOverlay(&const_cast<MemoryModel&>(controller->getMemoryModel())));
    LOGGER_LOG_DEBUG("MemoryControllerColumnOverlay created from controller: {}", toStringFull());
}

void MemoryControllerColumnOverlay::setMemoryModel(const MemoryModel& model) noexcept(true)
{
    memoryModel.reset(model.clone());
}

MemoryModel& MemoryControllerColumnOverlay::getMemoryModel() noexcept(true)
{
    return *memoryModel;
}

void MemoryControllerColumnOverlay::pegCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true)
{
    counters.pegCounter(counterId, value);
}

void MemoryControllerColumnOverlay::pegCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true)
{
    counters.pegCounter(counterId, value);
}

void MemoryControllerColumnOverlay::setCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true)
{
    counters.resetCounter(counterId);
    counters.pegCounter(counterId, value);
}

void MemoryControllerColumnOverlay::setCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true)
{
    counters.resetCounter(counterId);
    counters.pegCounter(counterId, value);
}

std::string MemoryControllerColumnOverlay::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryControllerColumnOverlay {") +
                           std::string(" .memoryModel = ") + memoryModel->toString() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryControllerColumnOverlay {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toString()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryControllerColumnOverlay::toStringFull(bool oneLine) const noexcept(true)
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
        return std::string(std::string("MemoryControllerColumnOverlay {") +
                           std::string(" .memoryModel = ") + memoryModel->toStringFull() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" .readCache = ") + writeQueueString +
                           std::string(" .writeCache = ") + readQueueString +
                           std::string(" .overwriteCache = ") + overwriteQueueStringDebug +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryControllerColumnOverlay {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toStringFull()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("\t.readCache = ") + writeQueueString + std::string("\n") +
                           std::string("\t.writeCache = ") + readQueueString + std::string("\n") +
                           std::string("\t.overwriteCache = ") + overwriteQueueStringDebug + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}
