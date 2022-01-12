#include <index/phantomIndex.hpp>

PhantomIndex::PhantomIndex(const char* name, Disk* disk, bool isSpecialBulkloadFeatureOn)
: DBIndex(name, disk, 0, 0), isSpecialBulkloadFeatureOn{isSpecialBulkloadFeatureOn}
{
    LOGGER_LOG_DEBUG("PhantomIndex created {}", toStringFull());
}


PhantomIndex::PhantomIndex(Disk* disk, bool isSpecialBulkloadFeatureOn)
: PhantomIndex("PhantomIndex", disk, isSpecialBulkloadFeatureOn)
{

}

std::string PhantomIndex::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("PhantomIndex {") +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("PhantomIndex {\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string PhantomIndex::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("PhantomIndex {") +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("PhantomIndex {\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

bool PhantomIndex::isBulkloadSupported() const noexcept(true)
{
    // In oryginal BPTree bulkload is unsupported, however user can decide if we want to enable this feature during index creation
    return isSpecialBulkloadFeatureOn;
}

double PhantomIndex::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = 0.0;

    numEntries += numOperations;

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("This is only Phantom!!! Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double PhantomIndex::bulkloadEntries(size_t numEntries) noexcept(true)
{
    if (!isBulkloadSupported())
    {
        LOGGER_LOG_WARN("This is only Phantom!!! Bulkload is unsupported");

        // unsupported
        return 0.0;
    }

    const double time = 0.0;

    this->numEntries += numEntries;

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("This is only Phantom!!! Inserted {} entries, took {}s", numEntries, time);

    return time;
}

double PhantomIndex::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = 0.0;

    numEntries -= realDeletion;

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("This is only Phantom!!! Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double PhantomIndex::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = 0.0;

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("This is only Phantom!!! Found {} entries, took {}s", numOperations, time);

    return time;
}

double PhantomIndex::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("This is only Phantom!!! selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double PhantomIndex::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    (void)numEntries;
    const double time = 0.0;

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("This is only Phantom!!! Found {} entries, took {}s", numOperations, time);

    return time;
}

double PhantomIndex::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("This is only Phantom!!! selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}

void PhantomIndex::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_DEBUG("Creating a topology now entries={}, inserting new {} entries, after we will have {} entries", this->numEntries, numEntries, this->numEntries + numEntries);

    this->numEntries += numEntries;
}