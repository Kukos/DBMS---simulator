#include <index/dsm.hpp>
#include <logger/logger.hpp>

#include <numeric>

double DSM::findKey() noexcept(true)
{
    double time = 0.0;

    // scan 1st column
    const uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->readBytes(addr, numEntries * columnsSize[0]);
    time += disk->flushCache();

    LOGGER_LOG_TRACE("Key found, took {}s", time);

    return time;
}

double DSM::insertEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        for (size_t j = 0; j < columnsSize.size(); ++j)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();

            // write at the end or in a predefined gap
            time += disk->overwriteBytes(addr, columnsSize[j]);
            time += disk->flushCache();
        }
        ++numEntries;
    }
    return time;
}

double DSM::bulkloadEntriesHelper(size_t numEntries) noexcept(true)
{
    double time = 0.0;

    for (size_t j = 0; j < columnsSize.size(); ++j)
    {
        const uintptr_t addr = disk->getCurrentMemoryAddr();

        // write at the end or in a predefined gap
        time += disk->writeBytes(addr, columnsSize[j] * numEntries);
        time += disk->flushCache();
    }

    this->numEntries += numEntries;

    return time;
}

double DSM::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    const size_t entriesToDelete = std::min(numOperations, numEntries);
    if (entriesToDelete < numOperations)
        LOGGER_LOG_DEBUG("To many entries to delete, capped to {} from {}", entriesToDelete, numOperations);

    for (size_t i = 0; i < entriesToDelete; ++i)
    {
        time += findKey();

        for (size_t j = 0; j < columnsSize.size(); ++j)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();

            // delete 1 entry
            time += disk->overwriteBytes(addr, columnsSize[j]);
            time += disk->flushCache();
        }

        --numEntries;
    }

    return time;
}

double DSM::findEntriesHelper(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    std::vector colCopy = columnsToFetch;
    std::sort(colCopy.begin(), colCopy.end());

    if (colCopy[0] != 0)
    {
        LOGGER_LOG_ERROR("You need key column!");
        return 0.0;
    }

    for (size_t i = 0; i < numOperations; ++i)
    {
        time += findKey();
        for (size_t j = 1; j < colCopy.size(); ++j)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, numEntries * columnsSize[colCopy[j]]);
            time += disk->flushCache();
        }
    }

    return time;
}

DSM::DSM(const char* name, Disk* disk, const std::vector<size_t>& columnsSize)
: DBIndexColumn(name, disk, columnsSize)
{
    LOGGER_LOG_DEBUG("DSM created {}", toStringFull());
}

DSM::DSM(Disk* disk, const std::vector<size_t>& columnsSize)
: DSM("DSM", disk, columnsSize)
{

}

std::string DSM::toString(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");


    if (oneLine)
        return std::string(std::string("DSM {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DSM {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string DSM::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");

    if (oneLine)
        return std::string(std::string("DSM {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DSM {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

bool DSM::isBulkloadSupported() const noexcept(true)
{
    return true;
}

double DSM::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double DSM::bulkloadEntries(size_t numEntries) noexcept(true)
{
    const double time = bulkloadEntriesHelper(numEntries);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Bulkloaded {} entries, took {}s", numEntries, time);

    return time;
}

double DSM::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double DSM::findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(columnsToFetch, 1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double DSM::findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(columnsToFetch, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double DSM::findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(columnsToFetch, numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double DSM::findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(columnsToFetch, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}

void DSM::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_DEBUG("Creating a topology now entries={}, inserting new {} entries, after we will have {} entries", this->numEntries, numEntries, this->numEntries + numEntries);

    this->numEntries += numEntries;
}