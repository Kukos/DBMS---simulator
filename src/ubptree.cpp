#include <index/ubptree.hpp>
#include <cmath>

size_t UBPTree::calculateNumOfNodes() const noexcept(true)
{
    return calculateNumOfInners() + calculateNumOfLeaves();
}

size_t UBPTree::calculateNumOfInners() const noexcept(true)
{
    const size_t entriesPerInner = nodeSize / sizePtr;
    size_t inners = 0;
    size_t nodes = calculateNumOfLeaves();

    while (nodes > 1)
    {
        nodes = static_cast<size_t>(ceil(static_cast<double>(nodes) / static_cast<double>(entriesPerInner)));
        inners += nodes;
    }

    return inners;
}

size_t UBPTree::calculateNumOfLeaves() const noexcept(true)
{
    return calculateNumOfLeavesForEntries(numEntries, sizeRecord);
}

size_t UBPTree::calculateNumOfLeavesForEntries(size_t numEntries, size_t entrySize) const noexcept(true)
{
    const size_t entriesPerLeaf = nodeSize / entrySize;

    return static_cast<size_t>(ceil(static_cast<double>(numEntries) / static_cast<double>(entriesPerLeaf)));
}

size_t UBPTree::calculateHeight() const noexcept(true)
{
    if (numEntries == 0)
        return 0;

    const size_t entriesPerInner = nodeSize / sizePtr;
    size_t height = 1;
    size_t nodes = calculateNumOfLeaves();

    while (nodes > 1)
    {
        nodes =  static_cast<size_t>(ceil(static_cast<double>(nodes) / static_cast<double>(entriesPerInner)));
        ++height;
    }

    return height;
}

double UBPTree::findLeaf() noexcept(true)
{
    double time = 0.0;

    if (height < 2)
    {
        LOGGER_LOG_TRACE("Height = {}, Leaf is a root, nothing to looking for", height);
        return 0.0;
    }

    // seek each lvl
    for (size_t i = 0; i < height - 1; ++i)
    {
        // find good pointer using binary search
        uintptr_t addr = disk->getCurrentMemoryAddr();
        const size_t maxEntries = nodeSize / sizePtr;

        size_t entryIndex = maxEntries / 2;
        while (entryIndex < maxEntries - 1)
        {
            time += disk->readBytes(entryIndex * sizePtr + addr, sizeKey);
            entryIndex += (maxEntries - entryIndex) / 2;
        }

        time += disk->readBytes((maxEntries / 2) * sizePtr, sizeof(void*));
        time += disk->flushCache();
    }

    LOGGER_LOG_DEBUG("Leaf found, took {}s", time);
    return time;
}

double UBPTree::insertEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        time += findLeaf();

        const size_t oldLeaves = calculateNumOfLeaves();
        const size_t oldInners = calculateNumOfInners();

        // note insert now to calculate new inners and leaves
        ++numEntries;

        // calculate changes in a structure to simulate split
        const size_t newLeaves = calculateNumOfLeaves();
        const size_t newInners = calculateNumOfInners();

        const size_t diffLeaves = newLeaves - oldLeaves;
        const size_t diffInners = newInners - oldInners;

        LOGGER_LOG_DEBUG("Inserting entry: oldLeaves = {}, oldInners = {}, newLeaves = {}, newInners = {}, diffLeaves = {}, diffInners = {}",
                         oldLeaves,
                         oldInners,
                         newLeaves,
                         newInners,
                         diffLeaves,
                         diffInners);

        uintptr_t addr = disk->getCurrentMemoryAddr();

        // we need into a gap (or at the end + update bitmap)
        time += disk->overwriteBytes(addr, sizeRecord);
        time += disk->overwriteBytes(addr + nodeSize - 1, 1);

        // split
        if (numEntries > 1 && diffLeaves > 0)
        {
            // read first half of current leaf
            time += disk->readBytes(addr + nodeSize / 2, nodeSize / 2);

            // flush cache from current leaf
            time += disk->flushCache();

            // go to another leaf
            addr = disk->getCurrentMemoryAddr();

            // write half a leaf to another leaf
            time += disk->writeBytes(addr, nodeSize / 2);

            // flush another leaf
            time += disk->flushCache();

            // write {key, void*} to inner
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
            time += disk->flushCache();

            // each new Inner = split on Inners Level and write Ptr to Level above
            for (size_t j = 0; j < diffInners; ++j)
            {
                // split Inner

                // get a half from inner
                addr = disk->getCurrentMemoryAddr();
                time += disk->readBytes(addr + nodeSize / 2, nodeSize / 2);
                time += disk->flushCache();

                // write down to another Inner
                addr = disk->getCurrentMemoryAddr();
                time += disk->writeBytes(addr, nodeSize / 2);
                time += disk->flushCache();

                // Write down Ptr to Level above
                addr = disk->getCurrentMemoryAddr();
                time += disk->readBytes(addr, nodeSize / 2);
                time += disk->overwriteBytes(addr, nodeSize / 2);
                time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
                time += disk->flushCache();
            }
        }
        // nothing changed in a structure
        else
            time += disk->flushCache();


        if (height != calculateHeight())
        {
            height = calculateHeight();
            LOGGER_LOG_DEBUG("Entry inserted, now entries = {}, height changed to {}", numEntries, height);
        }
        LOGGER_LOG_TRACE("Entry inserted now: entries = {}, height = {}, nodes = {}, inners = {}, leaves = {}", numEntries, height, calculateNumOfNodes(), calculateNumOfInners(), calculateNumOfLeaves());
    }

    return time;
}

double UBPTree::bulkloadEntriesHelper(size_t numEntries) noexcept(true)
{
    double time = 0.0;

    const size_t oldInners = calculateNumOfInners();
    const size_t oldLeaves = calculateNumOfLeaves();

    // note (ONLY FOR A WHILE) insert now to calculate new inners and leaves
    this->numEntries += numEntries;

    // calculate changes in a structure
    const size_t leavesForEntries = calculateNumOfLeavesForEntries(numEntries, sizeRecord);
    const size_t newInners = calculateNumOfInners();
    const size_t diffInners = newInners - oldInners;

    LOGGER_LOG_DEBUG("Bulkloading {} entries, oldLeaves = {}, oldInners = {}, leavesForEntries = {} newInners = {}, diffInners = {}",
                     numEntries,
                     oldLeaves,
                     oldInners,
                     leavesForEntries,
                     newInners,
                     diffInners);

    // need to revert note to works on a prev state (needed for findLeaf)
    this->numEntries -= numEntries;

    // Bulkload works as follows
    // 1. Build Leaves
    // 2. Find place for each leaf and insert inner to Inner Level
    // 3. Update Inners level if needed

    uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, nodeSize * leavesForEntries);
    time += disk->flushCache();

    for (size_t i = 0; i < leavesForEntries; ++i)
    {
        time += findLeaf();

        // write {key, void*} to inner
        addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
        time += disk->flushCache();
    }

    // each new Inner = split on Inners Level and write Ptr to Level above
    for (size_t i = 0; i < diffInners; ++i)
    {
        // split Inner

        // get a half from inner
        addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr + nodeSize / 2, nodeSize / 2);
        time += disk->flushCache();

        // write down to another Inner
        addr = disk->getCurrentMemoryAddr();
        time += disk->writeBytes(addr, nodeSize / 2);
        time += disk->flushCache();

        // Write down Ptr to Level above
        addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
        time += disk->flushCache();
    }

    // finally we can note this permanently
    this->numEntries += numEntries;

    if (height != calculateHeight())
    {
        height = calculateHeight();
        LOGGER_LOG_DEBUG("Entries inserted by bulkload, now entries = {}, height changed to {}", numEntries, height);
    }

    LOGGER_LOG_TRACE("Entries inserted by bulkload, now: entries = {}, height = {}, nodes = {}, inners = {}, leaves = {}", numEntries, height, calculateNumOfNodes(), calculateNumOfInners(), calculateNumOfLeaves());

    return time;
}

double UBPTree::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    const size_t entriesToDelete = std::min(numOperations, numEntries);
    if (entriesToDelete < numOperations)
        LOGGER_LOG_DEBUG("To many entries to delete, capped to {} from {}", entriesToDelete, numOperations);

    for (size_t i = 0; i < entriesToDelete; ++i)
    {
        // find place from we will delete entry

        time += findLeaf();

        const size_t oldLeaves = calculateNumOfLeaves();
        const size_t oldInners = calculateNumOfInners();

        // note delete now to calculate new inners and leaves
        --numEntries;

        // calculate changes in a structure to simulate merge
        const size_t newLeaves = calculateNumOfLeaves();
        const size_t newInners = calculateNumOfInners();

        const size_t diffLeaves = oldLeaves - newLeaves;
        const size_t diffInners = oldInners - newInners;

        LOGGER_LOG_DEBUG("Deleting entry, oldLeaves = {}, oldInners = {}, newLeaves = {}, newInners = {}, diffLeaves = {}, diffInners = {}",
                         oldLeaves,
                         oldInners,
                         newLeaves,
                         newInners,
                         diffLeaves,
                         diffInners);


        // delete = update bitmap at the end of node
        uintptr_t addr = disk->getCurrentMemoryAddr();
        time += disk->overwriteBytes(addr + nodeSize - 1, 1);
        time += disk->flushCache();

        // we need to merge leaves
        if (diffLeaves > 0)
        {
            // merge can occurs only in node which can help us (in avg nodeSize = maxNodeSize / 2)
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->writeBytes(addr, nodeSize);
            time += disk->flushCache();

            // we need to update Inner (simulate as delete and insert new Inner)
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
            time += disk->flushCache();

            for (size_t j = 0; j < diffInners; ++j)
            {
                // merge can occurs only in node which can help us (in avg nodeSize = maxNodeSize / 2)
                addr = disk->getCurrentMemoryAddr();
                time += disk->readBytes(addr, nodeSize / 2);
                time += disk->writeBytes(addr, nodeSize);
                time += disk->flushCache();

                // we need to update Inner (simulate as delete and insert new Inner)
                addr = disk->getCurrentMemoryAddr();
                time += disk->readBytes(addr, nodeSize / 2);
                time += disk->overwriteBytes(addr, nodeSize / 2);
                time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
                time += disk->flushCache();
            }
        }
    }

    if (height != calculateHeight())
    {
        height = calculateHeight();
        LOGGER_LOG_DEBUG("Entries deleted, now entries = {}, height changed to {}", numEntries, height);
    }

    LOGGER_LOG_TRACE("Entries deleted, now: entries = {}, height = {}, nodes = {}, inners = {}, leaves = {}", numEntries, height, calculateNumOfNodes(), calculateNumOfInners(), calculateNumOfLeaves());

    return time;
}

double UBPTree::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    if (numEntries == 0)
        return 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        time += findLeaf();

        // find good Key using linear search
        uintptr_t addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr, nodeSize);
        time += disk->flushCache();

        // Lets fck last not complete leaf its only a simulation! :)
        const size_t leavesToRead = calculateNumOfLeavesForEntries(numEntries - 1, sizeRecord);

        LOGGER_LOG_TRACE("entriesToRead = {}, reading {} full leaves of size = {}", numEntries, leavesToRead, nodeSize);
        for (size_t j = 0; j < leavesToRead; ++j)
        {
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize);
            time += disk->flushCache();
        }
    }

    LOGGER_LOG_TRACE("Entries {} found x {} times, took {}s", numEntries, numOperations, time);

    return time;
}

UBPTree::UBPTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, bool isSpecialBulkloadFeatureOn)
: DBIndex("UB+Tree", disk, sizeKey, sizeData), nodeSize{nodeSize}, sizePtr{sizeKey + sizeof(void*)}, height{0}, isSpecialBulkloadFeatureOn{isSpecialBulkloadFeatureOn}
{
    LOGGER_LOG_DEBUG("UBPTree created {}", toStringFull());
}

UBPTree::UBPTree(Disk* disk, size_t sizeKey, size_t sizeData, bool isSpecialBulkloadFeatureOn)
: UBPTree(disk, sizeKey, sizeData, disk->getLowLevelController().getPageSize(), isSpecialBulkloadFeatureOn)
{

}

size_t UBPTree::getHeight() const noexcept(true)
{
    return height;
}

bool UBPTree::isBulkloadSupported() const noexcept(true)
{
    // In oryginal UBPTree bulkload is unsupported, however user can decide if we want to enable this feature during index creation
    return isSpecialBulkloadFeatureOn;
}

double UBPTree::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double UBPTree::bulkloadEntries(size_t numEntries) noexcept(true)
{
    if (!isBulkloadSupported())
    {
        LOGGER_LOG_WARN("Bulkload is unsupported");

        // unsupported
        return 0.0;
    }

    const double time = bulkloadEntriesHelper(numEntries);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numEntries, time);

    return time;
}

double UBPTree::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double UBPTree::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double UBPTree::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double UBPTree::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double UBPTree::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}

std::string UBPTree::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("UBPTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .sizePtr = ") + std::to_string(sizePtr) +
                           std::string(" .height = ") + std::to_string(height) +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("UBPTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.sizePtr = ") + std::to_string(sizePtr) + std::string("\n") +
                           std::string("\t.height = ") + std::to_string(height) + std::string("\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string UBPTree::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("UBPTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .sizePtr = ") + std::to_string(sizePtr) +
                           std::string(" .height = ") + std::to_string(height) +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("UBPTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.sizePtr = ") + std::to_string(sizePtr) + std::string("\n") +
                           std::string("\t.height = ") + std::to_string(height) + std::string("\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}