#include <adaptiveMerging/adaptiveMerging.hpp>
#include <logger/logger.hpp>

#include <numeric>
#include <algorithm>
#include <iostream>

AdaptiveMerging::AMPartitionManager::AMPartition::AMPartition(size_t numEntries)
: numEntries{numEntries}
{
    LOGGER_LOG_TRACE("Partition created {}", toStringFull());
}

std::string AdaptiveMerging::AMPartitionManager::AMPartition::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AMPartition {") +
                           std::string(" .numEntries") + std::to_string(numEntries) +
                           std::string(" }"));
    else
        return std::string(std::string("AMPartition {\n") +
                           std::string("\t.numEntries = ") +  std::to_string(numEntries) + std::string("\n") +
                           std::string("}"));
}

std::string AdaptiveMerging::AMPartitionManager::AMPartition::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AMPartition {") +
                           std::string(" .numEntries") + std::to_string(numEntries) +
                           std::string(" }"));
    else
        return std::string(std::string("AMPartition {\n") +
                           std::string("\t.numEntries = ") +  std::to_string(numEntries) + std::string("\n") +
                           std::string("}"));
}

AdaptiveMerging::AMPartitionManager::AMPartitionManager(size_t numEntries, size_t recordSize, size_t partitionSize, enum AMUnsortedMemoryInvalidation invalidationType)
: AdaptiveMergingFramework::AMUnsortedMemoryManager(numEntries, recordSize, invalidationType), partitionSize{partitionSize}
{
    const size_t maxEntriesInParition = partitionSize / recordSize;
    size_t entriesToInsert = numEntries;

    while (entriesToInsert > 0)
    {
        const size_t entriesToParition = std::min(entriesToInsert, maxEntriesInParition);
        partitions.push_back(AMPartition(entriesToParition));

        entriesToInsert -= entriesToParition;
    }

    LOGGER_LOG_DEBUG("AMPartitionManager created {}", toStringFull());
}

std::string AdaptiveMerging::AMPartitionManager::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AMUnsortedMemoryManager {") +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" .partitionSize = ") + std::to_string(partitionSize) +
                           std::string(" .numPartitions = ") + std::to_string(partitions.size()) +
                           std::string(" }"));
    else
        return std::string(std::string("AMUnsortedMemoryManager {\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("\t.partitionSize = ") + std::to_string(partitionSize) + std::string("\n") +
                           std::string("\t.numPartitions = ") + std::to_string(partitions.size()) + std::string("\n") +
                           std::string("}"));
}

std::string AdaptiveMerging::AMPartitionManager::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringAMParitions = [](const std::string &accumulator, const AMPartition &partition)
    {
        return accumulator.empty() ? partition.toStringFull() : accumulator + "," + partition.toStringFull();
    };

    const std::string partitionString =  std::string("{") +
                                         std::accumulate(std::begin(partitions), std::end(partitions), std::string(), buildStringAMParitions) +
                                         std::string("}");

    if (oneLine)
        return std::string(std::string("AMUnsortedMemoryManager {") +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" .partitionSize = ") + std::to_string(partitionSize) +
                           std::string(" .numPartitions = ") + std::to_string(partitions.size()) +
                           std::string(" .partitions = ") + partitionString +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("AMUnsortedMemoryManager {\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("\t.partitionSize = ") + std::to_string(partitionSize) + std::string("\n") +
                           std::string("\t.numPartitions = ") + std::to_string(partitions.size()) + std::string("\n") +
                           std::string("\t.partitions = ") + partitionString  + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

void AdaptiveMerging::AMPartitionManager::cleanPartitions() noexcept(true)
{
    for (size_t i = 0; i < partitions.size(); ++i)
        partitions[i].numEntries = 0;

    numEntries = 0;
}

double AdaptiveMerging::AMPartitionManager::loadEntries(Disk *disk, size_t numEntries) noexcept(true)
{
    double lTime = 0.0;
    double iTime = 0.0;

    if (invalidationType == AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE)
    {
        // we need to simulate reading packed partitions, so calculate how many pages we should read at minimum, to simulate that less parts are better
        const size_t minPagesForEntries = (numEntries * recordSize + disk->getLowLevelController().getPageSize() - 1) / disk->getLowLevelController().getPageSize();
        const size_t minPagesForParitions = (this->numEntries * recordSize + partitionSize - 1) / partitionSize;

        // scan required entries
        uintptr_t addr = disk->getCurrentMemoryAddr();
        lTime += disk->readBytes(addr, numEntries * recordSize);
        lTime += disk->flushCache();

        // invalid data
        iTime += invalidEntries(disk, numEntries, partitionSize);

        // seek untouched parts
        if (minPagesForEntries < minPagesForParitions)
        {
            for (size_t i = 0; i < minPagesForParitions - minPagesForEntries; ++i)
            {
                uintptr_t addr = disk->getCurrentMemoryAddr();
                lTime += disk->readBytes(addr, 1);
                lTime += disk->flushCache();
            }
        }
    }
    else
    {
        // to randomize partitions sequences
        std::vector<size_t> pos;
        for (size_t i = 0; i < partitions.size(); ++i)
            pos.push_back(i);

        std::random_shuffle(pos.begin(), pos.end());

        // distribute entries through partitions
        std::unique_ptr<size_t[]> toLoad(new size_t[partitions.size()]);
        std::fill(toLoad.get(), toLoad.get() + partitions.size(), 0);

        size_t entriesToLoad = numEntries;
        while (entriesToLoad > 0)
        {
            for (size_t i = 0; i < pos.size(); ++i)
            {
                size_t ppos = pos[i];

                if (partitions[ppos].numEntries == 0)
                    continue;

                size_t entiesFromThisPartition = std::min(partitions[ppos].numEntries, numEntries / partitions.size());
                entiesFromThisPartition = std::max(entiesFromThisPartition, static_cast<size_t>(1));
                entiesFromThisPartition = std::min(entiesFromThisPartition, entriesToLoad);

                toLoad.get()[ppos] += entiesFromThisPartition;
                entriesToLoad -= entiesFromThisPartition;

                if (entriesToLoad == 0)
                    break;
            }
        }

        // load entries from partitions
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            partitions[i].numEntries -= toLoad.get()[i];
            uintptr_t addr = disk->getCurrentMemoryAddr();
            lTime += disk->readBytes(addr, toLoad.get()[i] * recordSize);
            lTime += disk->flushCache();

            if (invalidationType != AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL)
                iTime += invalidEntries(disk, toLoad.get()[i], partitionSize);
        }

        if (invalidationType == AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL)
            iTime += invalidEntries(disk, numEntries, partitionSize);

        const size_t minPagesForEntries = (numEntries * recordSize + disk->getLowLevelController().getPageSize() - 1) / disk->getLowLevelController().getPageSize();
        const size_t minPagesForParitions = (this->numEntries * recordSize + partitionSize - 1) / partitionSize;

        // seek untouched parts
        if (minPagesForEntries < minPagesForParitions)
        {
            for (size_t i = 0; i < minPagesForParitions - minPagesForEntries; ++i)
            {
                uintptr_t addr = disk->getCurrentMemoryAddr();
                lTime += disk->readBytes(addr, 1);
                lTime += disk->flushCache();
            }
        }
    }

    this->numEntries -= numEntries;

    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME, lTime);
    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Loading entries {}, took {}s, invalidation took {}s, now entries {}", numEntries, lTime, iTime, this->numEntries);

    return lTime + iTime;
}

double AdaptiveMerging::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        auto splitEntries = splitLoadForIndexAndUnsortedPart(numEntries);
        size_t loadFromIndex = splitEntries.first;
        size_t loadFromPartitions = splitEntries.second;

        if (loadFromIndex > 0)
            time += index->findRangeEntries(loadFromIndex);

        time += memoryManager->loadEntries(const_cast<Disk*>(&index->getDisk()), loadFromPartitions);
        if (index->isBulkloadSupported())
            time += index->bulkloadEntries(loadFromPartitions);
        else
            time += index->insertEntries(loadFromPartitions);

        // keeping partitions is a waste of time, copy all to index and get rid of them
        if (memoryManager->getNumEntries() <= copyTreshold)
        {
            if (index->isBulkloadSupported())
                time += index->bulkloadEntries(memoryManager->getNumEntries());
            else
                time += index->insertEntries(memoryManager->getNumEntries());

            dynamic_cast<AdaptiveMerging::AMPartitionManager*>(memoryManager.get())->cleanPartitions();
        }
    }

    LOGGER_LOG_TRACE("FindEntriesHelper: entries {}, operations {}, took {}s", numEntries, numOperations, time);

    return time;
}

double AdaptiveMerging::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    const size_t entriesToDelete = std::min(numOperations, getNumEntries());

    auto splitEntries = splitLoadForIndexAndUnsortedPart(entriesToDelete);
    size_t loadFromIndex = splitEntries.first;
    size_t loadFromPartitions = splitEntries.second;

    if (loadFromIndex > 0)
        time += index->deleteEntries(loadFromIndex);

    time += memoryManager->loadEntries(const_cast<Disk*>(&index->getDisk()), loadFromPartitions);

    // keeping partitions is a waste of time, copy all to index and get rid of them
    if (memoryManager->getNumEntries() <= copyTreshold)
    {
        if (index->isBulkloadSupported())
            time += index->bulkloadEntries(memoryManager->getNumEntries());
        else
            time += index->insertEntries(memoryManager->getNumEntries());

        dynamic_cast<AdaptiveMerging::AMPartitionManager*>(memoryManager.get())->cleanPartitions();
    }

    LOGGER_LOG_TRACE("DeleteEntriesHelper: operations {}, took {}s", numOperations, time);

    return time;
}

AdaptiveMerging::AdaptiveMerging(const char* name, DBIndex* index, AMPartitionManager* manager, size_t startingEntries)
: AdaptiveMergingFramework(name, index, manager, startingEntries)
{
    LOGGER_LOG_DEBUG("AdaptiveMerging created {}", toStringFull());
}

AdaptiveMerging::AdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize)
: AdaptiveMerging(name, index, new AMPartitionManager(startingEntries, index->getRecordSize(), partitionSize, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE), startingEntries)
{

}

AdaptiveMerging::AdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize)
: AdaptiveMerging("AdaptiveMerging", index, startingEntries, partitionSize)
{

}

AdaptiveMerging::AdaptiveMerging(DBIndex* index, AMPartitionManager* manager, size_t startingEntries)
: AdaptiveMerging("AdaptiveMerging", index, manager, startingEntries)
{

}

std::string AdaptiveMerging::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMerging {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMerging {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toString() + std::string("\n") +
                           std::string("\t.index = ") + index->toString() + std::string("\n") +
                           std::string("}"));}

std::string AdaptiveMerging::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMerging {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .startingEntries = ") + std::to_string(startingEntries) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMerging {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.startingEntries = ") + std::to_string(startingEntries) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toStringFull() + std::string("\n") +
                           std::string("\t.index = ") + index->toStringFull() + std::string("\n") +
                           std::string("}"));
}

double AdaptiveMerging::insertEntries(size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("AM: insertEntries {}", numOperations);
    return index->insertEntries(numOperations);
}

double AdaptiveMerging::bulkloadEntries(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_TRACE("AM: bulkloadEntries {}", numEntries);

    return index->bulkloadEntries(numEntries);
}

double AdaptiveMerging::deleteEntries(size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("AM: DeleteEntries {}", numOperations);

    double time = deleteEntriesHelper(numOperations);

    return time;
}

double AdaptiveMerging::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double AdaptiveMerging::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity) * numOperations);
}

double AdaptiveMerging::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double AdaptiveMerging::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity), numOperations);
}

void AdaptiveMerging::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    index->createTopologyAfterInsert(numEntries);
}