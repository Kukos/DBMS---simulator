#include <index/fdtree.hpp>
#include <logger/logger.hpp>
#include <numeric>

FDTree::FDLvl::FDLvl(size_t sizeInBytes, size_t recordSize, size_t lvl)
: sizeInBytes{sizeInBytes}, numEntries{0}, numEntriesToDelete{0}, maxEntries{sizeInBytes / recordSize}, lvl{lvl}
{
    if (lvl == static_cast<size_t>(-1))
        LOGGER_LOG_TRACE("FD HeadTree created {}", toStringFull());
    else
        LOGGER_LOG_TRACE("FD Lvl {} created {}", lvl, toStringFull());
}

std::string FDTree::FDLvl::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("FDLvl {") +
                           std::string(" .lvl = ") + std::to_string(lvl) +
                           std::string(" .sizeInBytes = ") + std::to_string(sizeInBytes) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .numEntriesToDelete = ") + std::to_string(numEntriesToDelete) +
                           std::string(" .maxEntries = ") + std::to_string(maxEntries) +
                           std::string(" }"));
    else
        return std::string(std::string("FDLvl {\n") +
                           std::string("\t.lvl = ") + std::to_string(lvl) + std::string("\n") +
                           std::string("\t.sizeInBytes = ") + std::to_string(sizeInBytes) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.numEntriesToDelete = ") + std::to_string(numEntriesToDelete) + std::string("\n") +
                           std::string("\t.maxEntries = ") + std::to_string(maxEntries) + std::string("\n") +
                           std::string("}"));
}

std::string FDTree::FDLvl::toStringFull(bool oneLine) const noexcept(true)
{
    return toString(oneLine);
}

bool FDTree::FDLvl::isFull() const noexcept(true)
{
    return numEntries + numEntriesToDelete >= maxEntries;
}

bool FDTree::FDLvl::willBeFullAfterMerge(const FDLvl& upper)
{
    const ssize_t entriesAfterMerge = static_cast<ssize_t>(numEntries) +
                                      static_cast<ssize_t>(upper.numEntries) +
                                      static_cast<ssize_t>(upper.numEntriesToDelete) +
                                      static_cast<ssize_t>(numEntriesToDelete) -
                                      std::min(numEntries, upper.numEntriesToDelete);

    return entriesAfterMerge >= static_cast<ssize_t>(maxEntries);
}

double FDTree::insertIntoHeadTree(size_t entries) noexcept(true)
{
    // headTree is in RAM (cost = 0)
    double time = 0.0;
    headTree.numEntries += entries;

    LOGGER_LOG_TRACE("{} entries inserted into headTree, now: ({} + {} = {})/{}, took {}s", entries, headTree.numEntries, headTree.numEntriesToDelete, headTree.numEntries + headTree.numEntriesToDelete, headTree.maxEntries, 0.0);

    if (headTree.isFull())
    {
        if (getHeight() == 0)
            addLevel();

        time += mergeHeadTree();
    }

    return time;
}

double FDTree::deleteFromHeadTree(size_t entries) noexcept(true)
{
    // headTree is in RAM (cost = 0)
    double time = 0.0;
    headTree.numEntriesToDelete += entries;

    LOGGER_LOG_TRACE("{} entriesToDelete inserted into headTree, now: ({} + {} = {})/{}, took {}s", entries, headTree.numEntries, headTree.numEntriesToDelete, headTree.numEntries + headTree.numEntriesToDelete, headTree.maxEntries, 0.0);

    if (headTree.isFull())
    {
        if (getHeight() == 0)
            addLevel();

        time += mergeHeadTree();
    }

    return time;
}

void FDTree::addLevel() noexcept(true)
{
    size_t lvlSize = headTree.sizeInBytes * lvlRatio;
    if (getHeight() > 0)
        lvlSize = levels[getHeight() - 1].sizeInBytes * lvlRatio;

    FDLvl lvl{lvlSize, getRecordSize(), getHeight()};
    levels.push_back(lvl);
}

double FDTree::mergeHeadTree() noexcept(true)
{
    double time = 0.0;

    auto lvl = std::ref(levels[0]);

    LOGGER_LOG_DEBUG("Merging FD HeadTree ({} + {} = {}) / {} with FD LVL{} ({} + {} = {}) / {}",
                     headTree.numEntries,
                     headTree.numEntriesToDelete,
                     headTree.numEntries + headTree.numEntriesToDelete,
                     headTree.maxEntries,
                     lvl.get().getLvl(),
                     lvl.get().numEntries,
                     lvl.get().numEntriesToDelete,
                     lvl.get().numEntries + lvl.get().numEntriesToDelete,
                     lvl.get().maxEntries);

    if (lvl.get().willBeFullAfterMerge(headTree))
    {
        if (getHeight() < 2)
            addLevel();

        const double mergeTime = mergeLevels(0, 1);
        lvl = std::ref(levels[0]);
        auto lower = std::ref(levels[1]);

        LOGGER_LOG_DEBUG("LVL{} merged with LVL{} took {}s, now LVL{} ({} + {} = {}) / {}, LVL{} ({} + {} = {}) / {}",
                         lvl.get().getLvl(),
                         lower.get().getLvl(),
                         mergeTime,
                         lvl.get().getLvl(),
                         lvl.get().numEntries,
                         lvl.get().numEntriesToDelete,
                         lvl.get().numEntries + lvl.get().numEntriesToDelete,
                         lvl.get().maxEntries,
                         lower.get().getLvl(),
                         lower.get().numEntries,
                         lower.get().numEntriesToDelete,
                         lower.get().numEntries + lower.get().numEntriesToDelete,
                         lower.get().maxEntries);

        time += mergeTime;
    }

    // how many entries (normal) will stay in lvl0
    const size_t entriesAfterMerge = lvl.get().numEntries > headTree.numEntriesToDelete ? lvl.get().numEntries - headTree.numEntriesToDelete + headTree.numEntries : headTree.numEntries;

    // how many entries (to delete) will stay in lvl0
    const size_t entriesToDeleteAfterMerge = headTree.numEntriesToDelete > lvl.get().numEntries ? headTree.numEntriesToDelete - lvl.get().numEntries + lvl.get().numEntriesToDelete : lvl.get().numEntriesToDelete;

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FD LVL{} {} bytes, writing into FD LVL{} {} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     lvl.get().getLvl(),
                     (lvl.get().numEntries + lvl.get().numEntriesToDelete) * getRecordSize(),
                     lvl.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());

    // read entries from lvl0
    uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->readBytes(addr, (lvl.get().numEntries + lvl.get().numEntriesToDelete) * getRecordSize());
    time += disk->flushCache();

    // write new level as lvl0
    addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());
    time += disk->flushCache();

    // update metadata
    headTree.numEntries = 0;
    headTree.numEntriesToDelete = 0;

    lvl.get().numEntries = entriesAfterMerge;
    lvl.get().numEntriesToDelete = entriesToDeleteAfterMerge;

    return time;
}

double FDTree::mergeLevels(size_t upperLvl, size_t lowerLvl) noexcept(true)
{
    double time = 0.0;

    auto upper = std::ref(levels[upperLvl]);
    auto lower = std::ref(levels[lowerLvl]);
    LOGGER_LOG_DEBUG("Merging FD LVL{} ({} + {} = {}) / {} with FD LVL{} ({} + {} = {}) / {}",
                     upper.get().getLvl(),
                     upper.get().numEntries,
                     upper.get().numEntriesToDelete,
                     upper.get().numEntries + upper.get().numEntriesToDelete,
                     upper.get().maxEntries,
                     lower.get().getLvl(),
                     lower.get().numEntries,
                     lower.get().numEntriesToDelete,
                     lower.get().numEntries + lower.get().numEntriesToDelete,
                     lower.get().maxEntries);

    if (lower.get().willBeFullAfterMerge(upper))
    {
        if (getHeight() < lower.get().getLvl() + 1)
            addLevel();

        const double mergeTime = mergeLevels(lowerLvl, lowerLvl + 1);
        upper = std::ref(levels[upperLvl]);
        lower = std::ref(levels[lowerLvl]);
        auto lowerLow = std::ref(levels[lowerLvl + 1]);

        LOGGER_LOG_DEBUG("FD LVL{} merged with FD LVL{} took {}s, now FD LVL{} ({} + {} = {}) / {}, FD LVL{} ({} + {} = {}) / {}",
                         lower.get().getLvl(),
                         lowerLow.get().getLvl(),
                         mergeTime,
                         lower.get().lvl,
                         lower.get().numEntries,
                         lower.get().numEntriesToDelete,
                         lower.get().numEntries + lower.get().numEntriesToDelete,
                         lower.get().maxEntries,
                         lowerLow.get().lvl,
                         lowerLow.get().numEntries,
                         lowerLow.get().numEntriesToDelete,
                         lowerLow.get().numEntries + lowerLow.get().numEntriesToDelete,
                         lowerLow.get().maxEntries);

        time += mergeTime;
    }

    // how many entries (normal) will stay in lvl lower
    const size_t entriesAfterMerge = lower.get().numEntries > upper.get().numEntriesToDelete ? lower.get().numEntries - upper.get().numEntriesToDelete + upper.get().numEntries : upper.get().numEntries;

    // how many entries (to delete) will stay in lvl lower
    const size_t entriesToDeleteAfterMerge = upper.get().numEntriesToDelete > lower.get().numEntries ? upper.get().numEntriesToDelete - lower.get().numEntries + lower.get().numEntriesToDelete : lower.get().numEntriesToDelete;

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FD LVL{} {} bytes, writting into FD LVL{} bytes, reading from FD LVL{} {} bytes, writting into FD LVL{} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     upper.get().getLvl(),
                     (upper.get().numEntries + upper.get().numEntriesToDelete) * getRecordSize(),
                     upper.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) / nodeSize * sizeof(void*),
                     lower.get().getLvl(),
                     (lower.get().numEntries + lower.get().numEntriesToDelete) * getRecordSize(),
                     lower.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());

    // read entries from lvl upper
    uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->readBytes(addr, (upper.get().numEntries + upper.get().numEntriesToDelete) * getRecordSize());
    time += disk->flushCache();

    // read entries from lvl lower
    addr = disk->getCurrentMemoryAddr();
    time += disk->readBytes(addr, (lower.get().numEntries + lower.get().numEntriesToDelete) * getRecordSize());
    time += disk->flushCache();

    // wrtie entries to lower
    addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());
    time += disk->flushCache();

    // write fences to upper
    addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, (entriesAfterMerge + entriesToDeleteAfterMerge) / nodeSize * sizeof(void*));
    time += disk->flushCache();

    // update metadata
    upper.get().numEntries = 0;
    upper.get().numEntriesToDelete = 0;

    lower.get().numEntries = entriesAfterMerge;
    lower.get().numEntriesToDelete = entriesToDeleteAfterMerge;

    return time;
}

double FDTree::insertEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        ++numEntries;

        time += insertIntoHeadTree(1);
    }

    return time;
}

double FDTree::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        // cannot delete more
        if (numEntries == 0)
            return time;

        --numEntries;

        time += deleteFromHeadTree(1);
    }

    return time;
}

double FDTree::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    if (getHeight() == 0)
    {
        LOGGER_LOG_TRACE("FindEntries in HeadTree, cost 0.0");

        return 0.0;
    }

    for (size_t i = 0; i < numOperations; ++i)
        for (size_t j = 0; j < getHeight(); ++j)
        {
            double lvlTime = 0.0;
            const size_t maxEntries = nodeSize / getRecordSize();
            uintptr_t addr = disk->getCurrentMemoryAddr();

            size_t entryIndex = maxEntries / 2;
            while (entryIndex < maxEntries - 1)
            {
                lvlTime += disk->readBytes(entryIndex * getRecordSize() + addr, sizeKey);
                entryIndex += (maxEntries - entryIndex) / 2;
            }

            if (j == getHeight() - 1)
                lvlTime += disk->readBytes((maxEntries / 2) * getRecordSize() + addr, numEntries == 1 ? sizeData : numEntries * getRecordSize());

            lvlTime += disk->flushCache();

            LOGGER_LOG_TRACE("Seek FD LVL{} took {}s", j + 1, lvlTime);
            time += lvlTime;
        }

    return time;
}

FDTree::FDTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t headTreeSize, size_t lvlRatio)
: DBIndex("FDTree", disk, sizeKey, sizeData), nodeSize{nodeSize}, lvlRatio{lvlRatio}, headTree{FDLvl(headTreeSize, sizeKey + sizeData, -1)}
{
    LOGGER_LOG_DEBUG("FDTree created {}", toStringFull());
}

FDTree::FDTree(Disk* disk, size_t sizeKey, size_t sizeData)
: FDTree(disk, sizeKey, sizeData, disk->getLowLevelController().getPageSize(), disk->getLowLevelController().getPageSize(), 10)
{

}

std::string FDTree::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("FDTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("FDTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string FDTree::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFDLvls = [](const std::string &accumulator, const FDTree::FDLvl &lvl)
    {
        return accumulator.empty() ? lvl.toStringFull() : accumulator + "," + lvl.toStringFull();
    };

    const std::string fdLvlsString =  std::string("{") +
                                      std::accumulate(std::begin(levels), std::end(levels), std::string(), buildStringFDLvls) +
                                      std::string("}");


    if (oneLine)
        return std::string(std::string("FDTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .headTree = ") + headTree.toStringFull() +
                           std::string(" .levels = ") + fdLvlsString +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("FDTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.headTree = ") + headTree.toStringFull() + std::string("\n") +
                           std::string("\t.levels = ") + fdLvlsString + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

size_t FDTree::getHeight() const noexcept(true)
{
    return levels.size();
}

const FDTree::FDLvl& FDTree::getFDLvl(size_t lvl) const noexcept(true)
{
    if (lvl == 0)
        return headTree;

    return levels[lvl - 1];
}

bool FDTree::isBulkloadSupported() const noexcept(true)
{
    // In oryginal FDTree bulkload is unsupported, use insert loop into buffered HeadTree
    return false;
}

double FDTree::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double FDTree::bulkloadEntries(size_t numOperations) noexcept(true)
{
    (void)numOperations;

    LOGGER_LOG_WARN("Bulkload is unsupported");

    // unsupported
    return 0.0;
}

double FDTree::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double FDTree::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double FDTree::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double FDTree::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double FDTree::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}