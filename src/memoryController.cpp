#include <storage/memoryController.hpp>
#include <logger/logger.hpp>
#include <algorithm>
#include <string>
#include <numeric>

bool MemoryController::isAddrAlignedToCacheLine(uintptr_t addr, size_t cacheLineSize) const noexcept(true)
{
    return addr % cacheLineSize == 0;
}

size_t MemoryController::addrToCacheLineIndex(uintptr_t addr, size_t cacheLineSize) const noexcept(true)
{
    return static_cast<size_t>(addr) / cacheLineSize;
}

MemoryController::MemoryController(MemoryModel* memoryModel)
: MemoryController(memoryModel, memoryModel->getPageSize(), memoryModel->getPageSize())
{

}

MemoryController::MemoryController(MemoryModel* memoryModel, size_t readCacheLineSize, size_t writeCacheLineSize)
: memoryModel{std::unique_ptr<MemoryModel>(memoryModel)}, readCacheLineSize{readCacheLineSize}, writeCacheLineSize{writeCacheLineSize}
{
    LOGGER_LOG_DEBUG("Memory controller created: {}", toStringFull());
}

MemoryController::MemoryController(const MemoryController& other)
: MemoryController((*other.memoryModel).clone(), other.readCacheLineSize, other.writeCacheLineSize)
{
    readCache = other.readCache;
    writeCache = other.writeCache;
    overwriteCache = other.overwriteCache;
    counters = other.counters;
}

MemoryController& MemoryController::operator=(const MemoryController& other)
{
    if (this == &other)
        return *this;

    readCacheLineSize = other.readCacheLineSize;
    writeCacheLineSize = other.writeCacheLineSize;
    readCache = other.readCache;
    writeCache = other.writeCache;
    overwriteCache = other.overwriteCache;
    counters = other.counters;
    memoryModel.reset((*other.memoryModel).clone());

    return *this;
}

const char* MemoryController::getModelName() const noexcept(true)
{
    return memoryModel->getModelName();
}

size_t MemoryController::getPageSize() const noexcept(true)
{
    return memoryModel->getPageSize();
}

size_t MemoryController::getBlockSize() const noexcept(true)
{
    return memoryModel->getBlockSize();
}

size_t MemoryController::getMemoryWearOut() const noexcept(true)
{
    return memoryModel->getMemoryWearOut();
}

uintptr_t MemoryController::getCurrentMemoryAddr() const noexcept(true)
{
    return 0;
}

void MemoryController::resetState() noexcept(true)
{
    memoryModel->resetState();
    counters = MemoryCounters();
}

double MemoryController::flushCache() noexcept(true)
{
    double flushWriteTime = 0.0;
    double flushOverWriteTime = 0.0;
    size_t bytesToWrite = 0;;

    // flush write Queue
    if (writeCache.size() > 0)
        bytesToWrite = writeCacheLineSize;

    auto buildStringFromPageQueue = [](const std::string &accumulator, const size_t &page)
    {
        return accumulator.empty() ? std::to_string(page) : accumulator + "," + std::to_string(page);
    };

    const std::string writeQueueString = std::accumulate(std::begin(writeCache), std::end(writeCache), std::string(), buildStringFromPageQueue);
    LOGGER_LOG_DEBUG("Flushing write queue: [{}] ...", writeQueueString);

    std::sort(writeCache.begin(), writeCache.end());
    for (size_t i = 1; i < writeCache.size(); ++i)
        // still contiguous memory
        if (writeCache[i - 1] + 1 == writeCache[i])
        {
            LOGGER_LOG_TRACE("Write queue page {:d} is still in contuguous area", i);
            bytesToWrite += writeCacheLineSize;
        }
        else // memory gap, need to write pages in Queue
        {
            const double flushTime = memoryModel->writeBytes(bytesToWrite);
            LOGGER_LOG_TRACE("Write queue page{:d} broke memory area, writing {:d} bytes took {}s", i, bytesToWrite, flushTime);

            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, flushTime);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, 1);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, bytesToWrite);

            flushWriteTime += flushTime;

            // bytes reseted but here we need to start counting new page
            bytesToWrite = writeCacheLineSize;
        }

    if (bytesToWrite > 0)
    {
        const double flushTime = memoryModel->writeBytes(bytesToWrite);
        LOGGER_LOG_TRACE("Write queue writing last pages {:d} bytes took {}s",  bytesToWrite, flushTime);

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, flushTime);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, bytesToWrite);

        flushWriteTime += flushTime;
    }

    LOGGER_LOG_DEBUG("Write queue flushed, took {}s", flushWriteTime);

    // flush overwrite Queue
    bytesToWrite = 0;
    size_t bytesToRead = 0;

    auto addBytesToRead = [this](size_t i)
    {
        size_t bytesToRead = 0.0;
        size_t logicalAddr = 0;
        while (logicalAddr < writeCacheLineSize)
        {
            const size_t physicalAddr = i * writeCacheLineSize + logicalAddr;
            const size_t readCacheLineIndex = addrToCacheLineIndex(physicalAddr, readCacheLineSize);

            // fetch page before overwrite
            if (overwriteCache[i].second[logicalAddr] == false && std::find(readCache.begin(), readCache.end(), readCacheLineIndex) == readCache.end())
            {
                LOGGER_LOG_TRACE("Overwritting page {:d} logical address {:d}, add {:d} bytes to read before overwrite", i, logicalAddr, readCacheLineSize);
                bytesToRead += readCacheLineSize;
                // go to the next page
                logicalAddr = (addrToCacheLineIndex(logicalAddr, readCacheLineSize) + 1) * readCacheLineSize;
            }
            else
                ++logicalAddr;
        }
        return bytesToRead;
    };

    if (overwriteCache.size() > 0)
    {
        bytesToWrite = writeCacheLineSize;
        bytesToRead += addBytesToRead(0);
    }

    std::sort(overwriteCache.begin(), overwriteCache.end());

    auto buildStringFromPageMapQueueDebug = [](const std::string &accumulator, const std::pair<size_t, std::vector<bool>> &pair)
    {
        return accumulator.empty() ? std::to_string(pair.first) : accumulator + "," + std::to_string(pair.first);
    };

    // to debug print only pages, to trace print also pageMap
    const std::string overwriteQueueStringDebug = std::accumulate(std::begin(overwriteCache), std::end(overwriteCache), std::string(), buildStringFromPageMapQueueDebug);
    LOGGER_LOG_DEBUG("Flushing overwrite queue: [{}] ...", overwriteQueueStringDebug);

    for (size_t i = 1; i < overwriteCache.size(); ++i)
        // still contiguous memory
        if (overwriteCache[i - 1].first + 1 == overwriteCache[i].first)
        {
            LOGGER_LOG_TRACE("Overwrite queue page {:d} is still in contuguous area", i);
            bytesToWrite += writeCacheLineSize;
            bytesToRead += addBytesToRead(i);
        }
        else // memory gap, need to write pages in Queue
        {
            const double flushReadTime = memoryModel->readBytes(bytesToRead);
            const double flushWriteTime = memoryModel->overwriteBytes(bytesToWrite);
            const double flushTime = flushReadTime + flushWriteTime;
            flushOverWriteTime += flushTime;

            LOGGER_LOG_TRACE("Overwrite queue page{:d} broke memory area, reading {:d} bytes, writing {:d} bytes took {}s", i, bytesToRead, bytesToWrite, flushTime);

            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, flushReadTime);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytesToRead);

            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, flushWriteTime);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, 1);
            counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, bytesToWrite);

            // bytes reseted but here we need to start counting new page
            bytesToWrite = writeCacheLineSize;
            bytesToRead = addBytesToRead(i);
        }

    if (bytesToWrite > 0)
    {
        const double flushWriteTime = memoryModel->overwriteBytes(bytesToWrite);
        flushOverWriteTime += flushWriteTime;

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, flushWriteTime);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, bytesToWrite);

        LOGGER_LOG_TRACE("Overwrite queue writing last pages {:d} bytes took {}s",  bytesToWrite, flushWriteTime);
    }

    if (bytesToRead > 0)
    {
        const double flushReadTime = memoryModel->readBytes(bytesToRead);
        flushOverWriteTime += flushReadTime;

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, flushReadTime);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytesToRead);

        LOGGER_LOG_TRACE("Overwrite queue reading last pages {:d} bytes took {}s", bytesToRead, flushReadTime);
    }

    LOGGER_LOG_DEBUG("Overwrite queue flushed, took {}s", flushOverWriteTime);

    // clear cache
    readCache.clear();
    writeCache.clear();
    overwriteCache.clear();

    return flushWriteTime + flushOverWriteTime;
}

double MemoryController::readBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    double time = 0.0;
    const size_t cacheLineStartIndex = addrToCacheLineIndex(addr, readCacheLineSize);
    const size_t cacheLineEndIndex = addrToCacheLineIndex(addr + static_cast<uintptr_t>(bytes - 1), readCacheLineSize);
    std::vector<size_t> readQueue;

    auto addPageToReadCache = [this, &readQueue]()
    {
        const size_t bytesToRead = readQueue.size() * readCacheLineSize;
        double time = memoryModel->readBytes(bytesToRead);

        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, time);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
        counters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytesToRead);

        const std::string readQueueString = std::accumulate(std::begin(readQueue),
                                                            std::end(readQueue),
                                                            std::string(),
                                                            [](const std::string &accumulator, const size_t &page)
                                                            {
                                                                return accumulator.empty() ? std::to_string(page) : accumulator + "," + std::to_string(page);
                                                            });

        LOGGER_LOG_DEBUG("Fetching readQueue: [{}] to cache, took {}s", readQueueString, time);
        readCache.insert(readCache.end(), readQueue.begin(), readQueue.end());
        readQueue.clear();

        return time;
    };

    for (size_t i = cacheLineStartIndex; i <= cacheLineEndIndex; ++i)
        // CacheLine not fetched yet, add cacheLine to Queue
        if (std::find(readCache.begin(), readCache.end(), i) == readCache.end())
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... did not found, add to readQueue", i);
            readQueue.push_back(i);
        }
        else // cacheLine already fetched, so we break contiguous memory region, fetch all lines from queue
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... found in cache", i);
            if (readQueue.size() != 0)
                time += addPageToReadCache();
        }

    if (readQueue.size() != 0)
        time += addPageToReadCache();

    return time;
}

double MemoryController::writeBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t cacheLineStartIndex = addrToCacheLineIndex(addr, writeCacheLineSize);
    const size_t cacheLineEndIndex = addrToCacheLineIndex(addr + static_cast<uintptr_t>(bytes - 1), writeCacheLineSize);

    for (size_t i = cacheLineStartIndex; i <= cacheLineEndIndex; ++i)
        if (std::find(writeCache.begin(), writeCache.end(), i) == writeCache.end())
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... did not found, add to writeQueue", i);
            writeCache.push_back(i);
        }
        else
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... found in cache", i);
        }

    // waiting for flushing the cache, so now only  add to queue (cost 0)
    return 0.0;
}

double MemoryController::overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    if (bytes == 0)
        return 0.0;

    const size_t cacheLineStartIndex = addrToCacheLineIndex(addr, writeCacheLineSize);
    const size_t cacheLineEndIndex = addrToCacheLineIndex(addr + static_cast<uintptr_t>(bytes - 1), writeCacheLineSize);

    for (size_t i = cacheLineStartIndex; i <= cacheLineEndIndex; ++i)
    {
        auto cacheIt = std::find_if(overwriteCache.begin(), overwriteCache.end(), [i](const std::pair<size_t, std::vector<bool>>& elem){ return elem.first == i; });
        if (cacheIt == overwriteCache.end())
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... did not found, add to overwriteQueue", i);
            std::vector<bool> emptyPage = std::vector<bool>(writeCacheLineSize, false);
            std::pair<size_t, std::vector<bool>> pageMapPair = std::make_pair(i, emptyPage);
            overwriteCache.push_back(pageMapPair);
            cacheIt = std::find_if(overwriteCache.begin(), overwriteCache.end(), [i](const std::pair<size_t, std::vector<bool>>& elem){ return elem.first == i; });
        }
        else
        {
            LOGGER_LOG_DEBUG("CacheLine(page) {:d} requested ... found in cache", i);
        }

        std::vector<bool> pageMap = (*cacheIt).second;
        const size_t index = std::distance(overwriteCache.begin(), cacheIt);

        // read only one page
        if (i == cacheLineStartIndex && i == cacheLineEndIndex)
        {
            const size_t jEnd = isAddrAlignedToCacheLine(addr + static_cast<uintptr_t>(bytes), writeCacheLineSize) ? (writeCacheLineSize - 1) : ((addr + static_cast<uintptr_t>(bytes - 1)) % writeCacheLineSize);
            for (size_t j = addr % writeCacheLineSize; j <= jEnd; ++j)
                pageMap[j] = true;
        }
        // first page read from the middle
        else if (i == cacheLineStartIndex && !isAddrAlignedToCacheLine(addr, writeCacheLineSize))
        {
            for (size_t j = addr % writeCacheLineSize; j < writeCacheLineSize; ++j)
                pageMap[j] = true;
        }
        // last page read from the middle
        else if (i == cacheLineEndIndex && !isAddrAlignedToCacheLine(addr + static_cast<uintptr_t>(bytes), writeCacheLineSize))
        {
            for (size_t j = 0; j <= (addr + static_cast<uintptr_t>(bytes - 1)) % writeCacheLineSize; ++j)
                pageMap[j] = true;
        }
        // first or last or middle page read from the beggining to the end
        else
            pageMap = std::vector<bool>(writeCacheLineSize, true);

        overwriteCache[index].second = pageMap;
    }

    // waiting for flushing the cache, so now only  add to queue (cost 0)
    return 0.0;
}

std::string MemoryController::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryController {") +
                           std::string(" .memoryModel = ") + memoryModel->toString() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryController {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toString()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("}"));
}

std::string MemoryController::toStringFull(bool oneLine) const noexcept(true)
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
        return std::string(std::string("MemoryController {") +
                           std::string(" .memoryModel = ") + memoryModel->toStringFull() +
                           std::string(" .readCacheLineSize = ") + std::to_string(readCacheLineSize) +
                           std::string(" .writeCacheLineSize = ") + std::to_string(writeCacheLineSize) +
                           std::string(" .readCache = ") + writeQueueString +
                           std::string(" .writeCache = ") + readQueueString +
                           std::string(" .overwriteCache = ") + overwriteQueueStringDebug +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryController {\n") +
                           std::string("\t.memoryModel = ") + memoryModel->toStringFull()  + std::string("\n") +
                           std::string("\t.readCacheLineSize = ") + std::to_string(readCacheLineSize) + std::string("\n") +
                           std::string("\t.writeCacheLineSize = ") + std::to_string(writeCacheLineSize) + std::string("\n") +
                           std::string("\t.readCache = ") + writeQueueString + std::string("\n") +
                           std::string("\t.writeCache = ") + readQueueString + std::string("\n") +
                           std::string("\t.overwriteCache = ") + overwriteQueueStringDebug + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

std::pair<std::string, double> MemoryController::getCounter(enum MemoryCounters::MemoryCountersD counterId) const noexcept(true)
{
    return counters.getCounter(counterId);
}

std::pair<std::string, long> MemoryController::getCounter(enum MemoryCounters::MemoryCountersL counterId) const noexcept(true)
{
    return counters.getCounter(counterId);
}

void MemoryController::resetCounter(enum MemoryCounters::MemoryCountersL counterId) noexcept(true)
{
    counters.resetCounter(counterId);
}

void MemoryController::resetCounter(enum MemoryCounters::MemoryCountersD counterId) noexcept(true)
{
    counters.resetCounter(counterId);
}

void MemoryController::resetAllCounters() noexcept(true)
{
    counters.resetAllCounters();
}