#include <index/cbptree.hpp>
#include <cmath>

size_t CBPTree::calculateNumOfNodes() const noexcept(true)
{
    return calculateNumOfInners() + calculateNumOfLeaves();
}

size_t CBPTree::calculateNumOfInners() const noexcept(true)
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

size_t CBPTree::calculateNumOfLeaves() const noexcept(true)
{
    return calculateNumOfLeavesForEntries(numEntries, sizeRecord);
}

size_t CBPTree::calculateNumOfLeavesForEntries(size_t numEntries, size_t entrySize) const noexcept(true)
{
    const size_t entriesPerLeaf = nodeSize / entrySize;

    return static_cast<size_t>(ceil(static_cast<double>(numEntries) / static_cast<double>(entriesPerLeaf)));
}

size_t CBPTree::calculateHeight() const noexcept(true)
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

double CBPTree::findLeaf() noexcept(true)
{
    double time = 0.0;

    if (height < 2)
    {
        LOGGER_LOG_TRACE("Height = {}, Leaf is a root, but have Overflow nodes", height);

        for (size_t i = 0; i < overflowMaxNumber; ++i)
        {
            const uintptr_t addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr + nodeSize - sizeRecord, sizeKey);
            time += disk->flushCache();
        }

        LOGGER_LOG_DEBUG("Leaf found, took {}s", time);
        return time;
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

    for (size_t i = 0; i < overflowMaxNumber; ++i)
    {
        const uintptr_t addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr + nodeSize - sizeRecord, sizeKey);
        time += disk->flushCache();
    }


    LOGGER_LOG_DEBUG("Leaf found, took {}s", time);
    return time;
}

double CBPTree::insertEntriesHelper(size_t numOperations) noexcept(true)
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

        // add buff only when we have >1 entries
        insertOFBufferLeaf += (numEntries - 1) == 0 ? 0 : diffLeaves;
        insertOFBufferInner += (numEntries - 1) == 0 ? 0 : diffInners;

        LOGGER_LOG_DEBUG("Inserting entry: oldLeaves = {}, oldInners = {}, newLeaves = {}, newInners = {}, diffLeaves = {}, diffInners = {}, insertOFBufferLeaf = {}, insertOFBufferInner = {}",
                         oldLeaves,
                         oldInners,
                         newLeaves,
                         newInners,
                         diffLeaves,
                         diffInners,
                         insertOFBufferLeaf,
                         insertOFBufferInner);

        uintptr_t addr = disk->getCurrentMemoryAddr();

        // we need to insert into sorted leaf, make a gap by move in avg half a leaf
        time += disk->readBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr, nodeSize / 2);

        // insert into gap
        time += disk->overwriteBytes(addr + nodeSize / 2, sizeRecord);

        // split (add new OverFlow node)
        if (numEntries > 1 && diffLeaves > 0)
        {
            // read first half of current leaf
            time += disk->readBytes(addr, nodeSize / 2);

            // flush cache from current leaf
            time += disk->flushCache();

            // go to another leaf
            addr = disk->getCurrentMemoryAddr();

            // write half a leaf to another leaf
            time += disk->writeBytes(addr, nodeSize / 2);

            // flush another leaf
            time += disk->flushCache();
        } // nothing changed in a structure
        else
            time += disk->flushCache();

        // can't postopne inners update, OF will be normal node, update Inner
        if (insertOFBufferLeaf >= overflowMaxNumber)
        {
            // write overflowMaxNumber * {key, void*} to inner
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr + nodeSize / 2, overflowMaxNumber * sizePtr);
            time += disk->flushCache();

            // Inners are not buffered, I have buffer to make Inner update in a right time (when OF buffer has been flushed)
            // each new Inner = split on Inners Level and write Ptr to Level above
            for (size_t j = 0; j < insertOFBufferInner; ++j)
            {
                // split Inner

                // get a half from inner
                addr = disk->getCurrentMemoryAddr();
                time += disk->readBytes(addr, nodeSize / 2);
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

            insertOFBufferInner = 0;
            insertOFBufferLeaf -= overflowMaxNumber;
        }


        if (height != calculateHeight())
        {
            height = calculateHeight();
            LOGGER_LOG_DEBUG("Entry inserted, now entries = {}, height changed to {}", numEntries, height);
        }
        LOGGER_LOG_TRACE("Entry inserted now: entries = {}, height = {}, nodes = {}, inners = {}, leaves = {}", numEntries, height, calculateNumOfNodes(), calculateNumOfInners(), calculateNumOfLeaves());
    }

    return time;
}

double CBPTree::bulkloadEntriesHelper(size_t numEntries) noexcept(true)
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
        time += disk->readBytes(addr, nodeSize / 2);
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

double CBPTree::deleteEntriesHelper(size_t numOperations) noexcept(true)
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

        deleteOFBufferLeaf += diffLeaves;
        deleteOFBufferInner += diffInners;

        LOGGER_LOG_DEBUG("Deleting entry, oldLeaves = {}, oldInners = {}, newLeaves = {}, newInners = {}, diffLeaves = {}, diffInners = {}, deleteOFBufferLeaf = {}, deleteOFBufferInner = {}",
                         oldLeaves,
                         oldInners,
                         newLeaves,
                         newInners,
                         diffLeaves,
                         diffInners,
                         deleteOFBufferLeaf,
                         deleteOFBufferInner);


        // delete = overwrite in avg half node to overwrite medium element
        uintptr_t addr = disk->getCurrentMemoryAddr();
        time += disk->readBytes(addr, nodeSize / 2);
        time += disk->overwriteBytes(addr, nodeSize / 2);
        time += disk->flushCache();

        // we need to merge leaf with OverFlow node
        if (diffLeaves > 0)
        {
            // merge can occurs only in node which can help us (in avg nodeSize = maxNodeSize / 2)
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->writeBytes(addr, nodeSize);
            time += disk->flushCache();
        }

        // can't postopne inners update, OF deleted, needto update Inner
        if (deleteOFBufferLeaf >= overflowMaxNumber)
        {
            // we need to update Inner (simulate as delete and insert new Inner)
            addr = disk->getCurrentMemoryAddr();
            time += disk->readBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr, nodeSize / 2);
            time += disk->overwriteBytes(addr + nodeSize / 2, sizePtr);
            time += disk->flushCache();

            // Inners are not buffered, I have buffer to make Inner update in a right time (when OF buffer has been flushed)
            for (size_t j = 0; j < deleteOFBufferInner; ++j)
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

            deleteOFBufferInner = 0;
            deleteOFBufferLeaf -= overflowMaxNumber;
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

double CBPTree::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    if (numEntries == 0)
        return 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        time += findLeaf();

        // find good Key using binary search
        uintptr_t addr = disk->getCurrentMemoryAddr();
        const size_t maxEntries = nodeSize / sizeRecord;

        size_t entryIndex = maxEntries / 2;
        while (entryIndex < maxEntries - 1)
        {
            time += disk->readBytes(entryIndex * sizeRecord + addr, sizeKey);
            entryIndex += (maxEntries - entryIndex) / 2;
        }

        time += disk->readBytes((maxEntries / 2) * sizeRecord, sizeData);
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

CBPTree::CBPTree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, bool isSpecialBulkloadFeatureOn)
: DBIndex(name, disk, sizeKey, sizeData), nodeSize{nodeSize}, sizePtr{sizeKey + sizeof(void*)}, height{0}, isSpecialBulkloadFeatureOn{isSpecialBulkloadFeatureOn}, insertOFBufferLeaf{0}, insertOFBufferInner{0}, deleteOFBufferLeaf{0}, deleteOFBufferInner{0}
{
    LOGGER_LOG_DEBUG("CBPTree created {}", toStringFull());
}

CBPTree::CBPTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, bool isSpecialBulkloadFeatureOn)
: CBPTree("CB+Tree", disk, sizeKey, sizeData, nodeSize, isSpecialBulkloadFeatureOn)
{

}

CBPTree::CBPTree(Disk* disk, size_t sizeKey, size_t sizeData, bool isSpecialBulkloadFeatureOn)
: CBPTree(disk, sizeKey, sizeData, disk->getLowLevelController().getPageSize(), isSpecialBulkloadFeatureOn)
{

}

CBPTree::CBPTree(const char*name, Disk* disk, size_t sizeKey, size_t sizeData, bool isSpecialBulkloadFeatureOn)
: CBPTree(name, disk, sizeKey, sizeData, disk->getLowLevelController().getPageSize(), isSpecialBulkloadFeatureOn)
{

}

size_t CBPTree::getHeight() const noexcept(true)
{
    return height;
}

bool CBPTree::isBulkloadSupported() const noexcept(true)
{
    // In oryginal CBPTree bulkload is unsupported, however user can decide if we want to enable this feature during index creation
    return isSpecialBulkloadFeatureOn;
}

double CBPTree::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double CBPTree::bulkloadEntries(size_t numEntries) noexcept(true)
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

double CBPTree::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double CBPTree::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double CBPTree::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double CBPTree::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double CBPTree::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}

std::string CBPTree::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("CBPTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .sizePtr = ") + std::to_string(sizePtr) +
                           std::string(" .height = ") + std::to_string(height) +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .overflowMaxNumber = ") + std::to_string(overflowMaxNumber) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("CBPTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.sizePtr = ") + std::to_string(sizePtr) + std::string("\n") +
                           std::string("\t.height = ") + std::to_string(height) + std::string("\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.overflowMaxNumber = ") + std::to_string(overflowMaxNumber) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string CBPTree::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("CBPTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .sizePtr = ") + std::to_string(sizePtr) +
                           std::string(" .height = ") + std::to_string(height) +
                           std::string(" .isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) +
                           std::string(" .overflowMaxNumber = ") + std::to_string(overflowMaxNumber) +
                           std::string(" .insertOFBufferLeaf = ") + std::to_string(insertOFBufferLeaf) +
                           std::string(" .insertOFBufferInner = ") + std::to_string(insertOFBufferInner) +
                           std::string(" .deleteOFBufferLeaf = ") + std::to_string(deleteOFBufferLeaf) +
                           std::string(" .deleteOFBufferInner = ") + std::to_string(deleteOFBufferInner) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("CBPTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.sizePtr = ") + std::to_string(sizePtr) + std::string("\n") +
                           std::string("\t.height = ") + std::to_string(height) + std::string("\n") +
                           std::string("\t.isSpecialBulkloadFeatureOn = ") + std::to_string(isSpecialBulkloadFeatureOn) + std::string("\n") +
                           std::string("\t.overflowMaxNumber = ") + std::to_string(overflowMaxNumber) + std::string("\n") +
                           std::string("\t.insertOFBufferLeaf = ") + std::to_string(insertOFBufferLeaf) + std::string("\n") +
                           std::string("\t.insertOFBufferInner = ") + std::to_string(insertOFBufferInner) + std::string("\n") +
                           std::string("\t.deleteOFBufferLeaf = ") + std::to_string(deleteOFBufferLeaf) + std::string("\n") +
                           std::string("\t.deleteOFBufferInner = ") + std::to_string(deleteOFBufferInner) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

void CBPTree::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_DEBUG("Creating a topology now entries={}, height = {}, inserting new {} entries, after we will have {} entries", this->numEntries, height, numEntries, this->numEntries + numEntries);

    this->numEntries += numEntries;

    if (height != calculateHeight())
    {
        height = calculateHeight();
        LOGGER_LOG_DEBUG("Entries inserted by createTopology, now entries = {}, height changed to {}", numEntries, height);
    }
}