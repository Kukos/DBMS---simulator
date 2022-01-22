#include <adaptiveMerging/adaptiveMergingFramework.hpp>
#include <logger/logger.hpp>

#include <string>

AdaptiveMergingFramework::AMUnsortedMemoryManager::AMUnsortedMemoryManager(size_t numEntries, size_t recordSize, enum AMUnsortedMemoryInvalidation invalidationType)
: numEntries{numEntries}, recordSize{recordSize}, invalidationType{invalidationType}
{
    LOGGER_LOG_DEBUG("AMUnsortedMemoryManager created {}", toStringFull());
}

std::string AdaptiveMergingFramework::AMUnsortedMemoryManager::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AMUnsortedMemoryManager {") +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" }"));
    else
        return std::string(std::string("AMUnsortedMemoryManager {\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("}"));
}

std::string AdaptiveMergingFramework::AMUnsortedMemoryManager::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AMUnsortedMemoryManager {") +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("AMUnsortedMemoryManager {\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

double AdaptiveMergingFramework::AMUnsortedMemoryManager::invalidEntries(Disk* disk, size_t numEntries, size_t nodeSize) noexcept(true)
{
    double time = 0.0;

    if (numEntries == 0)
    {
        LOGGER_LOG_TRACE("NumEntries = 0, invalidation costs 0.0s");
        return 0.0;
    }

    switch (invalidationType)
    {
        case AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE:
        {
            for (size_t i = 0; i < (numEntries * recordSize + nodeSize - 1) / nodeSize; ++i)
            {
                const uintptr_t addr = disk->getCurrentMemoryAddr();

                time += disk->readBytes(addr, nodeSize / 4);
                time += disk->overwriteBytes(addr, nodeSize / 4);
                time += disk->flushCache();
            }
            break;
        }
        case AM_UNSORTED_MEMORY_INVALIDATION_FLAG:
        {
            uintptr_t addr = disk->getCurrentMemoryAddr() + recordSize;
            for (size_t i = 0; i < numEntries; ++i)
            {
                time += disk->overwriteBytes(addr - 1, 1);
                addr += recordSize;
            }

            time += disk->flushCache();

            break;
        }
        case AM_UNSORTED_MEMORY_INVALIDATION_BITMAP:
        {
            const size_t entriesPerNode = nodeSize / recordSize;
            size_t entriesToInvalid = numEntries;

            while (entriesToInvalid > 0)
            {
                const size_t entriesToInvalidInThisNode = std::min(entriesToInvalid, entriesPerNode);

                const uintptr_t addr = disk->getCurrentMemoryAddr();
                time += disk->overwriteBytes(addr, (entriesToInvalidInThisNode + 7) / 8);
                time += disk->flushCache();

                entriesToInvalid -= entriesToInvalidInThisNode;
            }

            break;
        }
        case AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL:
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();

            time += disk->overwriteBytes(addr, sizeof(size_t) * 2);
            time += disk->flushCache();

            break;
        }
        default:
            break;
    }

    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME, time);
    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Invalidation type {}, entries {}, nodeSize {}, took {}s", invalidationType, numEntries, nodeSize, time);
    return time;
}

AdaptiveMergingFramework::AdaptiveMergingFramework(const char* name, DBIndex* index, AMUnsortedMemoryManager* manager, size_t startingEntries)
: DBIndex(name, index->getDisk().clone(), index->getKeySize(), index->getDataSize()), index{std::unique_ptr<DBIndex>(index)}, memoryManager{std::unique_ptr<AMUnsortedMemoryManager>(manager)}, startingEntries{startingEntries}, rng{ std::mt19937(seed)}
{
    LOGGER_LOG_DEBUG("AdaptiveMergingFramework created {}", toStringFull());
}

std::string AdaptiveMergingFramework::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMergingFramework {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMergingFramework {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toString() + std::string("\n") +
                           std::string("\t.index = ") + index->toString() + std::string("\n") +
                           std::string("}"));
}

std::string AdaptiveMergingFramework::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMergingFramework {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .startingEntries = ") + std::to_string(startingEntries) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMergingFramework {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.startingEntries = ") + std::to_string(startingEntries) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toStringFull() + std::string("\n") +
                           std::string("\t.index = ") + index->toStringFull() + std::string("\n") +
                           std::string("}"));
}

AdaptiveMergingFramework::AdaptiveMergingFramework(const AdaptiveMergingFramework& other)
: AdaptiveMergingFramework(other.name, (*other.index).clone(), (*other.memoryManager).clone(), other.startingEntries)
{
    rng = other.rng;
}

AdaptiveMergingFramework& AdaptiveMergingFramework::operator=(const AdaptiveMergingFramework& other)
{
    if (this == &other)
        return *this;

    DBIndex::operator=(other);
    rng = other.rng;
    index.reset((*other.index).clone());
    memoryManager.reset((*other.memoryManager).clone());

    return *this;
}

std::pair<size_t, size_t> AdaptiveMergingFramework::splitLoadForIndexAndUnsortedPart(size_t numEntries) noexcept(true)
{
    if (numEntries > getNumEntries())
    {
        LOGGER_LOG_ERROR("entriesToLoad {} >= numEntriesInAMF {}", numEntries, getNumEntries());
        return std::pair<size_t, size_t>(0, 0);
    }

    size_t fromIndex = 0;

    std::uniform_int_distribution<size_t> randomizer(0, getNumEntries());
    if (index->getNumEntries() > 0)
    {
        for (size_t i = 0; i < numEntries; ++i)
            if (randomizer(rng) >= memoryManager->numEntries)
            {
                ++fromIndex;
                if (fromIndex >= index->getNumEntries())
                    break;
            }
    }
    else
    {
        LOGGER_LOG_DEBUG("Index is empty, all entries should be loaded from unsorted part");
    }

    // sanity check
    fromIndex = std::min(fromIndex, index->getNumEntries());

    const size_t fromUnsorted = std::min(numEntries - fromIndex, memoryManager->numEntries);
    fromIndex = numEntries - fromUnsorted;

    LOGGER_LOG_DEBUG("SplitEntries: fromIndex: {}, fromUnsortedPart: {}", fromIndex, fromUnsorted);
    return std::pair<size_t, size_t>(fromIndex, fromUnsorted);
}


std::pair<std::string, double> AdaptiveMergingFramework::getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true)
{
    switch (counterId)
    {
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_AVG_TIME:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_AVG_TIME:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_TIME:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME);
        }
        case IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME:
        {
            return std::pair<std::string, double>(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).first, index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second + memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME).second);
        }

        default:
            break;
    }

    return index->getCounter(counterId);
}

std::pair<std::string, long> AdaptiveMergingFramework::getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true)
{
    switch (counterId)
    {
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_OPERATIONS:
        {
            return memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);
        }
        case IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS:
        {
            return std::pair<std::string, double>(index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).first, index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second + memoryManager->counters.getCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS).second);
        }
        default:
            break;
    }

    return index->getCounter(counterId);
}

void AdaptiveMergingFramework::resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true)
{
        switch (counterId)
    {
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_AVG_TIME:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_AVG_TIME:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_TIME:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME);
            break;
        }
        case IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME:
        {
            index->resetCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME);
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME);
            break;
        }

        default:
            break;
    }

    index->resetCounter(counterId);
}

void AdaptiveMergingFramework::resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true)
{
    switch (counterId)
    {
        case IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);
            break;
        }
        case IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_OPERATIONS:
        {
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);
            break;
        }
        case IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS:
        {
            index->resetCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS);
            memoryManager->counters.resetCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS);
            break;
        }
        default:
            break;
    }

    index->resetCounter(counterId);
}

void AdaptiveMergingFramework::resetAllCounters() noexcept(true)
{
    memoryManager->counters.resetAllCounters();
    index->resetAllCounters();
}