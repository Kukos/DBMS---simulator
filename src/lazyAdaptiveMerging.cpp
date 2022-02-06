#include <adaptiveMerging/lazyAdaptiveMerging.hpp>
#include <logger/logger.hpp>

#include <numeric>
#include <algorithm>
#include <iostream>
#include <tuple>

LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartitionLEB::LAMPartitionLEB(size_t entries, size_t maxEntries)
: curValidEntries{entries}, curInvalidEntries{0}, maxEntries{maxEntries}
{
    LOGGER_LOG_DEBUG("LEB created {}", toStringFull());
}

LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartitionLEB::LAMPartitionLEB(size_t maxEntries)
: LAMPartitionLEB(maxEntries, maxEntries)
{

}

std::string LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartitionLEB::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LAMPartitionLEB {") +
                           std::string(" .curValidEntries") + std::to_string(curValidEntries) +
                           std::string(" .curInvalidEntries") + std::to_string(curInvalidEntries) +
                           std::string(" .maxEntries") + std::to_string(maxEntries) +
                           std::string(" .usage") + std::to_string(usage()) +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartitionLEB {\n") +
                           std::string("\t.curValidEntries") + std::to_string(curValidEntries) + std::string("\n") +
                           std::string("\t.curInvalidEntries") + std::to_string(curInvalidEntries) +  std::string("\n") +
                           std::string("\t.maxEntries") + std::to_string(maxEntries) +  std::string("\n") +
                           std::string("\t.usage") + std::to_string(usage()) +  std::string("\n") +
                           std::string("}"));
}


std::string LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartitionLEB::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LAMPartitionLEB {") +
                           std::string(" .curValidEntries") + std::to_string(curValidEntries) +
                           std::string(" .curInvalidEntries") + std::to_string(curInvalidEntries) +
                           std::string(" .maxEntries") + std::to_string(maxEntries) +
                           std::string(" .usage") + std::to_string(usage()) +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartitionLEB {\n") +
                           std::string("\t.curValidEntries") + std::to_string(curValidEntries) + std::string("\n") +
                           std::string("\t.curInvalidEntries") + std::to_string(curInvalidEntries) +  std::string("\n") +
                           std::string("\t.maxEntries") + std::to_string(maxEntries) +  std::string("\n") +
                           std::string("\t.usage") + std::to_string(usage()) +  std::string("\n") +
                           std::string("}"));
}

double LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartitionLEB::usage() const noexcept(true)
{
    return static_cast<double>(curValidEntries) / static_cast<double>(maxEntries);
}


LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::LAMPartition(size_t entries, size_t maxEntriesInBlock)
: numEntries{entries}, maxEntriesInBlock{maxEntriesInBlock}
{
    size_t entriesToInsert = entries;
    while (entriesToInsert > 0)
    {
        const size_t entriesToInsertToLEB = std::min(entriesToInsert, maxEntriesInBlock);
        blocks.push_back(LAMPartitionLEB(entriesToInsertToLEB, maxEntriesInBlock));

        entriesToInsert -= entriesToInsertToLEB;
    }

    LOGGER_LOG_DEBUG("LAMPartition Created {}", toStringFull());
}

std::string LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LAMPartition {") +
                           std::string(" .numEntries") + std::to_string(numEntries) +
                           std::string(" .maxEntriesInBlock") + std::to_string(maxEntriesInBlock) +
                           std::string(" .numBlocks = ") + std::to_string(blocks.size()) +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartition {\n") +
                           std::string("\t.numEntries = ") +  std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.maxEntriesInBlock = ") +  std::to_string(maxEntriesInBlock) + std::string("\n") +
                           std::string("\t.numBlocks = ") +  std::to_string(blocks.size()) + std::string("\n") +
                           std::string("}"));
}

std::string LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromBlocks = [](const std::string &accumulator, const LAMPartitionLEB& leb)
    {
        return accumulator.empty() ? leb.toStringFull() : accumulator + "," + leb.toStringFull();
    };

    const std::string blocksString =  std::string("{") +
                                      std::accumulate(std::begin(blocks), std::end(blocks), std::string(), buildStringFromBlocks) +
                                      std::string("}");

    if (oneLine)
        return std::string(std::string("LAMPartition {") +
                           std::string(" .numEntries") + std::to_string(numEntries) +
                           std::string(" .maxEntriesInBlock") + std::to_string(maxEntriesInBlock) +
                           std::string(" .numBlocks = ") + std::to_string(blocks.size()) +
                           std::string(" .blocks") + blocksString +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartition {\n") +
                           std::string("\t.numEntries = ") +  std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.maxEntriesInBlock = ") +  std::to_string(maxEntriesInBlock) + std::string("\n") +
                           std::string("\t.numBlocks = ") +  std::to_string(blocks.size()) + std::string("\n") +
                           std::string("\t.blocks = ") + blocksString + std::string("\n") +
                           std::string("}"));
}


void LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::reset()  noexcept(true)
{
    blocks.clear();
    numEntries = 0;
}

void LazyAdaptiveMerging::LAMPartitionManager::LAMPartition::addLeb(const LAMPartitionLEB& leb)  noexcept(true)
{
    blocks.push_back(leb);
    numEntries += leb.getCurValidEntries();
}


LazyAdaptiveMerging::LAMPartitionManager::LAMPartitionManager(size_t numEntries, size_t recordSize, size_t partitionSize, size_t blockSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold, enum AMUnsortedMemoryInvalidation invalidationType)
: AdaptiveMergingFramework::AMUnsortedMemoryManager(numEntries, recordSize, invalidationType), partitionSize{partitionSize}, reorganizationMaxBlocks{reorganizationMaxBlocks}, reorganizationBlocksTreshold{reorganizationBlocksTreshold}, lebUsageTreshold{lebUsageTreshold}, reorganizationCounter{0}
{
    maxEntriesInParition = std::min(partitionSize / recordSize, (partitionSize / blockSize) * (blockSize / recordSize));
    maxEntriesInBlock = blockSize / recordSize;
    size_t entriesToInsert = numEntries;

    while (entriesToInsert > 0)
    {
        const size_t entriesToParition = std::min(entriesToInsert, maxEntriesInParition);
        partitions.push_back(LAMPartition(entriesToParition, maxEntriesInBlock));

        entriesToInsert -= entriesToParition;
    }

    LOGGER_LOG_DEBUG("LAMPartitionManager created {}", toStringFull());
}

std::string LazyAdaptiveMerging::LAMPartitionManager::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LAMPartitionManager {") +
                           std::string(" .reorganizationCounter = ") + std::to_string(reorganizationCounter) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" .reorganizationMaxBlocks = ") + std::to_string(reorganizationMaxBlocks) +
                           std::string(" .reorganizationBlocksTreshold = ") + std::to_string(reorganizationBlocksTreshold) +
                           std::string(" .lebUsageTreshold = ") + std::to_string(lebUsageTreshold) +
                           std::string(" .partitionSize = ") + std::to_string(partitionSize) +
                           std::string(" .maxEntriesInParition = ") + std::to_string(maxEntriesInParition) +
                           std::string(" .numPartitions = ") + std::to_string(partitions.size()) +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartitionManager {\n") +
                           std::string("\t.reorganizationCounter = ") + std::to_string(reorganizationCounter) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("\t.reorganizationMaxBlocks = ") + std::to_string(reorganizationMaxBlocks) + std::string("\n") +
                           std::string("\t.reorganizationBlocksTreshold = ") + std::to_string(reorganizationBlocksTreshold) + std::string("\n") +
                           std::string("\t.lebUsageTreshold = ") + std::to_string(lebUsageTreshold) + std::string("\n") +
                           std::string("\t.partitionSize = ") + std::to_string(partitionSize) + std::string("\n") +
                           std::string("\t.maxEntriesInParition = ") + std::to_string(maxEntriesInParition) + std::string("\n") +
                           std::string("\t.numPartitions = ") + std::to_string(partitions.size()) + std::string("\n") +
                           std::string("}"));
}

std::string LazyAdaptiveMerging::LAMPartitionManager::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringAMParitions = [](const std::string &accumulator, const LAMPartition &partition)
    {
        return accumulator.empty() ? partition.toStringFull() : accumulator + "," + partition.toStringFull();
    };

    const std::string partitionString =  std::string("{") +
                                         std::accumulate(std::begin(partitions), std::end(partitions), std::string(), buildStringAMParitions) +
                                         std::string("}");

    if (oneLine)
        return std::string(std::string("LAMPartitionManager {") +
                           std::string(" .reorganizationCounter = ") + std::to_string(reorganizationCounter) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .recordSize = ") + std::to_string(recordSize) +
                           std::string(" .invalidationType = ") + std::to_string(invalidationType) +
                           std::string(" .reorganizationMaxBlocks = ") + std::to_string(reorganizationMaxBlocks) +
                           std::string(" .reorganizationBlocksTreshold = ") + std::to_string(reorganizationBlocksTreshold) +
                           std::string(" .lebUsageTreshold = ") + std::to_string(lebUsageTreshold) +
                           std::string(" .partitionSize = ") + std::to_string(partitionSize) +
                           std::string(" .maxEntriesInParition = ") + std::to_string(maxEntriesInParition) +
                           std::string(" .numPartitions = ") + std::to_string(partitions.size()) +
                           std::string(" .partitions = ") + partitionString +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("LAMPartitionManager {\n") +
                           std::string("\t.reorganizationCounter = ") + std::to_string(reorganizationCounter) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.recordSize = ") + std::to_string(recordSize) + std::string("\n") +
                           std::string("\t.invalidationType = ") + std::to_string(invalidationType) + std::string("\n") +
                           std::string("\t.reorganizationMaxBlocks = ") + std::to_string(reorganizationMaxBlocks) + std::string("\n") +
                           std::string("\t.reorganizationBlocksTreshold = ") + std::to_string(reorganizationBlocksTreshold) + std::string("\n") +
                           std::string("\t.lebUsageTreshold = ") + std::to_string(lebUsageTreshold) + std::string("\n") +
                           std::string("\t.partitionSize = ") + std::to_string(partitionSize) + std::string("\n") +
                           std::string("\t.maxEntriesInParition = ") + std::to_string(maxEntriesInParition) + std::string("\n") +
                           std::string("\t.numPartitions = ") + std::to_string(partitions.size()) + std::string("\n") +
                           std::string("\t.partitions = ") + partitionString  + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

bool LazyAdaptiveMerging::LAMPartitionManager::isLebToDelete(const LAMPartition::LAMPartitionLEB& leb) const noexcept(true)
{
    const double usage = leb.usage();
    return usage < lebUsageTreshold;
}

size_t LazyAdaptiveMerging::LAMPartitionManager::getNumberOfLebs() const noexcept(true)
{
    size_t lebs = 0;

    for (size_t i = 0; i < partitions.size(); ++i)
        lebs += partitions[i].getBlocksRef().size();

    return lebs;
}

bool LazyAdaptiveMerging::LAMPartitionManager::canReorganizePartitions() const noexcept(true)
{
    std::vector<LAMPartition::LAMPartitionLEB> allBlocks;
    double possibleLebsToUtilize = 0.0;

    for (size_t i = 0; i < partitions.size(); ++i)
        allBlocks.insert(allBlocks.end(), partitions[i].getBlocksRef().begin(), partitions[i].getBlocksRef().end());

    // sort ascending usage
    std::sort(allBlocks.begin(), allBlocks.end(), [](const LAMPartition::LAMPartitionLEB& a, const LAMPartition::LAMPartitionLEB& b) -> bool { return a.usage() < b.usage(); });

    for (size_t i = 0; i < std::min(allBlocks.size(), reorganizationMaxBlocks); ++i)
        if (isLebToDelete(allBlocks[i]))
            possibleLebsToUtilize += 1.0 - allBlocks[i].usage(); // our gain is a full block - usage = how much we dont need to copy

    LOGGER_LOG_DEBUG("possibleLebsToUtilize = {}, Treshold = {}, decision {}", possibleLebsToUtilize, reorganizationBlocksTreshold, possibleLebsToUtilize >= static_cast<double>(reorganizationBlocksTreshold));

    return possibleLebsToUtilize >= static_cast<double>(reorganizationBlocksTreshold);
}

double LazyAdaptiveMerging::LAMPartitionManager::reorganizePartitions(Disk* disk, size_t recordSize) noexcept(true)
{
    double time = 0.0;

    using LebTuple = std::tuple<size_t, size_t, LAMPartition::LAMPartitionLEB>;

    std::vector<LebTuple> allBlocks;
    std::vector<LebTuple> toKeepBlocks;

    size_t entriesToCopy = 0;

    for (size_t i = 0; i < partitions.size(); ++i)
        for (size_t j = 0; j < partitions[i].getBlocksRef().size(); ++j)
            allBlocks.push_back(LebTuple(i, j, partitions[i].getBlocksRef()[j]));

    // sort ascending usage
    std::sort(allBlocks.begin(), allBlocks.end(), [](const LebTuple& a, const LebTuple& b) -> bool { return std::get<2>(a).usage() < std::get<2>(b).usage(); });

    for (size_t i = 0; i < std::min(allBlocks.size(), reorganizationMaxBlocks); ++i)
        if (isLebToDelete(std::get<2>(allBlocks[i])))
        {
            const size_t enteriesToLoad = std::get<2>(allBlocks[i]).getCurValidEntries();

            const uintptr_t addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, enteriesToLoad * recordSize);
            time += disk->flushCache();

            entriesToCopy += enteriesToLoad;

            LOGGER_LOG_TRACE("Reorganization: delete LEB [{}][{}] with {} entries",  std::get<0>(allBlocks[i]),  std::get<1>(allBlocks[i]), enteriesToLoad);
        }
        else
            toKeepBlocks.push_back(allBlocks[i]);

    // sort toKeepBlocks by <partitionNumber, LebNumber>
    std::sort(toKeepBlocks.begin(), toKeepBlocks.end(), [](const LebTuple& a, const LebTuple& b) -> bool { return (std::get<0>(a) < std::get<0>(b)) || (std::get<0>(a) == std::get<0>(b) && std::get<1>(a) < std::get<1>(b));  });

    // reset All Partitions
    for (size_t i = 0; i < partitions.size(); ++i)
        partitions[i].reset();

    // restore paritions
    for (size_t i = 0; i < toKeepBlocks.size(); ++i)
        partitions[std::get<0>(toKeepBlocks[i])].addLeb(std::get<2>(toKeepBlocks[i]));

    LOGGER_LOG_DEBUG("Reorganization: before remove, partitions size = {}", partitions.size());

    // erase partitions without any blocks
    partitions.erase(std::remove_if(partitions.begin(), partitions.end(), [](const LAMPartition& p){ return p.getBlocksRef().size() == 0; }), partitions.end());

    LOGGER_LOG_DEBUG("Reorganization: after remove, partitions size = {}, adding {} entries", partitions.size(), entriesToCopy);


    // create new partitions from entries
    while (entriesToCopy > 0)
    {
        const size_t entriesToParition = std::min(entriesToCopy, maxEntriesInParition);
        partitions.push_back(LAMPartition(entriesToParition, maxEntriesInBlock));

        entriesToCopy -= entriesToParition;

        const uintptr_t addr = disk->getCurrentMemoryAddr();
        time += disk->writeBytes(addr, entriesToParition * recordSize);
        time += disk->flushCache();
    }

    LOGGER_LOG_DEBUG("Reorganization: after adding, partitions size = {}", partitions.size());

    LOGGER_LOG_DEBUG("Reorganization took {}s", time);

    ++reorganizationCounter;

    return time;
}

void LazyAdaptiveMerging::LAMPartitionManager::cleanPartitions() noexcept(true)
{
    partitions.clear();

    numEntries = 0;
}

double LazyAdaptiveMerging::LAMPartitionManager::loadEntries(Disk *disk, size_t numEntries) noexcept(true)
{
    double lTime = 0.0;
    double iTime = 0.0;

    // to randomize partitions sequences
    std::vector<size_t> pos;
    for (size_t i = 0; i < partitions.size(); ++i)
        pos.push_back(i);

    std::random_shuffle(pos.begin(), pos.end());

    // distribute entries through partitions
    std::unique_ptr<size_t[]> toLoad(new size_t[partitions.size()]);
    std::fill(toLoad.get(), toLoad.get() + partitions.size(), 0);

    LOGGER_LOG_DEBUG("LOAD {} entries", numEntries);

    size_t entriesToLoad = numEntries;
    while (entriesToLoad > 0)
        for (size_t i = 0; i < pos.size(); ++i)
        {
            const size_t ppos = pos[i];

            if (partitions[ppos].numEntries == 0)
                continue;

            size_t entiesFromThisPartition = std::min(partitions[ppos].numEntries, numEntries / partitions.size() + 1);
            entiesFromThisPartition = std::max(entiesFromThisPartition, static_cast<size_t>(1));
            entiesFromThisPartition = std::min(entiesFromThisPartition, entriesToLoad);


            toLoad.get()[ppos] += entiesFromThisPartition;
            entriesToLoad -= entiesFromThisPartition;

            // load from LEBs
            std::uniform_int_distribution<size_t> randomizer(0, partitions[ppos].getBlocksRef().size() - 1);
            size_t lebPos = randomizer(*rng);

            while (entiesFromThisPartition > 0)
            {
                const size_t entriesLoadFromLeb = std::min(entiesFromThisPartition,  partitions[ppos].getBlocksRef()[lebPos].getCurValidEntries());
                partitions[ppos].readEntriesFromLebPos(entriesLoadFromLeb, lebPos);
                LOGGER_LOG_DEBUG("READ {} from LEB {} at PART {}, now usage = {}", entriesLoadFromLeb, lebPos, ppos,  partitions[ppos].getBlocksRef()[lebPos].usage());

                lebPos = (lebPos + 1) % partitions[ppos].getBlocksRef().size();

                entiesFromThisPartition -= entriesLoadFromLeb;
                partitions[ppos].numEntries -= entriesLoadFromLeb;
            }

            if (entriesToLoad == 0)
                break;
        }

    // load entries from partitions
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        uintptr_t addr = disk->getCurrentMemoryAddr();
        lTime += disk->readBytes(addr, toLoad.get()[i] * recordSize);
        lTime += disk->flushCache();
    }

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

    this->numEntries -= numEntries;

    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME, lTime);
    counters.pegCounter(AdaptiveMergingCounters::ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_DEBUG("Loading entries {}, took {}s, invalidation took {}s, now entries {}", numEntries, lTime, iTime, this->numEntries);

    return lTime + iTime;
}

LazyAdaptiveMerging::LazyAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold)
: AdaptiveMergingFramework(name, index, new LAMPartitionManager(startingEntries, index->getRecordSize(), partitionSize, index->getDisk().getLowLevelController().getBlockSize(), reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL), startingEntries)
{
    LOGGER_LOG_DEBUG("LazyAdaptiveMerging created {}", toStringFull());
    dynamic_cast<LazyAdaptiveMerging::LAMPartitionManager*>(memoryManager.get())->setRng(&rng);
}

LazyAdaptiveMerging::LazyAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold)
: LazyAdaptiveMerging("LazyAdaptiveMerging", index, startingEntries, partitionSize, reorganizationMaxBlocks, reorganizationBlocksTreshold, lebUsageTreshold)
{

}

std::string LazyAdaptiveMerging::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LazyAdaptiveMerging {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("LazyAdaptiveMerging {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toString() + std::string("\n") +
                           std::string("\t.index = ") + index->toString() + std::string("\n") +
                           std::string("}"));}

std::string LazyAdaptiveMerging::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LazyAdaptiveMerging {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .startingEntries = ") + std::to_string(startingEntries) +
                           std::string(" .numEntries") + std::to_string(getNumEntries()) +
                           std::string(" .memoryManager = ") + memoryManager->toString() +
                           std::string(" .index = ") + index->toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("LazyAdaptiveMerging {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.startingEntries = ") + std::to_string(startingEntries) + std::string("\n") +
                           std::string("\t.numEntries = ") +  std::to_string(getNumEntries()) + std::string("\n") +
                           std::string("\t.memoryManager = ") + memoryManager->toStringFull() + std::string("\n") +
                           std::string("\t.index = ") + index->toStringFull() + std::string("\n") +
                           std::string("}"));
}

double LazyAdaptiveMerging::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    const size_t entriesToDelete = std::min(numOperations, getNumEntries());

    auto splitEntries = splitLoadForIndexAndUnsortedPart(entriesToDelete);
    size_t loadFromIndex = splitEntries.first;
    size_t loadFromPartitions = splitEntries.second;

    if (loadFromIndex > 0)
        time += index->deleteEntries(loadFromIndex);

    time += memoryManager->loadEntries(const_cast<Disk*>(&index->getDisk()), loadFromPartitions);

    LazyAdaptiveMerging::LAMPartitionManager* manager =  dynamic_cast<LazyAdaptiveMerging::LAMPartitionManager*>(memoryManager.get());

    // keeping partitions is a waste of time, copy all to index and get rid of them
    if (memoryManager->getNumEntries() <= copyTreshold)
    {
        if (index->isBulkloadSupported())
            time += index->bulkloadEntries(memoryManager->getNumEntries());
        else
            time += index->insertEntries(memoryManager->getNumEntries());

       manager->cleanPartitions();
    }

    if (manager->canReorganizePartitions())
        time += manager->reorganizePartitions(const_cast<Disk*>(&index->getDisk()), getRecordSize());

    LOGGER_LOG_TRACE("DeleteEntriesHelper: operations {}, took {}s", numOperations, time);

    return time;
}

double LazyAdaptiveMerging::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
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

        LazyAdaptiveMerging::LAMPartitionManager* manager =  dynamic_cast<LazyAdaptiveMerging::LAMPartitionManager*>(memoryManager.get());


        // keeping partitions is a waste of time, copy all to index and get rid of them
        if (memoryManager->getNumEntries() <= copyTreshold)
        {
            if (index->isBulkloadSupported())
                time += index->bulkloadEntries(memoryManager->getNumEntries());
            else
                time += index->insertEntries(memoryManager->getNumEntries());

            manager->cleanPartitions();
        }

        if (manager->canReorganizePartitions())
            time += manager->reorganizePartitions(const_cast<Disk*>(&index->getDisk()), getRecordSize());
    }

    LOGGER_LOG_TRACE("FindEntriesHelper: entries {}, operations {}, took {}s", numEntries, numOperations, time);

    return time;
}


double LazyAdaptiveMerging::insertEntries(size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("LAM: insertEntries {}", numOperations);
    return index->insertEntries(numOperations);
}

double LazyAdaptiveMerging::bulkloadEntries(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_TRACE("LAM: bulkloadEntries {}", numEntries);

    return index->bulkloadEntries(numEntries);
}

double LazyAdaptiveMerging::deleteEntries(size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("LAM: DeleteEntries {}", numOperations);

    double time = deleteEntriesHelper(numOperations);

    return time;
}

double LazyAdaptiveMerging::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double LazyAdaptiveMerging::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity) * numOperations);
}

double LazyAdaptiveMerging::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double LazyAdaptiveMerging::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(getNumEntries()) * selectivity), numOperations);
}

void LazyAdaptiveMerging::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    index->createTopologyAfterInsert(numEntries);
}