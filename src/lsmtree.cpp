#include <index/lsmtree.hpp>
#include <logger/logger.hpp>
#include <numeric>

LSMTree::LSMLvl::LSMLvl(size_t sizeInBytes, size_t recordSize, size_t lvl, size_t nodeSize)
: sizeInBytes{sizeInBytes}, sizeInNodes{(sizeInBytes + nodeSize - 1)/ nodeSize},  numEntries{0}, numEntriesToDelete{0}, maxEntries{sizeInBytes / recordSize}, lvl{lvl}
{
    if (lvl == static_cast<size_t>(-1))
        LOGGER_LOG_TRACE("LSM BufferTree created {}", toStringFull());
    else
        LOGGER_LOG_TRACE("LSM Lvl {} created {}", lvl, toStringFull());
}

std::string LSMTree::LSMLvl::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LSMLvl {") +
                           std::string(" .lvl = ") + std::to_string(lvl) +
                           std::string(" .sizeInBytes = ") + std::to_string(sizeInBytes) +
                           std::string(" .sizeInNodes = ") + std::to_string(sizeInNodes) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .numEntriesToDelete = ") + std::to_string(numEntriesToDelete) +
                           std::string(" .maxEntries = ") + std::to_string(maxEntries) +
                           std::string(" }"));
    else
        return std::string(std::string("LSMLvl {\n") +
                           std::string("\t.lvl = ") + std::to_string(lvl) + std::string("\n") +
                           std::string("\t.sizeInBytes = ") + std::to_string(sizeInBytes) + std::string("\n") +
                           std::string("\t.sizeInNodes = ") + std::to_string(sizeInNodes) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.numEntriesToDelete = ") + std::to_string(numEntriesToDelete) + std::string("\n") +
                           std::string("\t.maxEntries = ") + std::to_string(maxEntries) + std::string("\n") +
                           std::string("}"));
}

std::string LSMTree::LSMLvl::toStringFull(bool oneLine) const noexcept(true)
{
    return toString(oneLine);
}

bool LSMTree::LSMLvl::isFull() const noexcept(true)
{
    return numEntries + numEntriesToDelete >= maxEntries;
}

bool LSMTree::LSMLvl::willBeFullAfterMerge(const LSMLvl& upper)
{
    const ssize_t entriesAfterMerge = static_cast<ssize_t>(numEntries) +
                                      static_cast<ssize_t>(upper.numEntries) +
                                      static_cast<ssize_t>(upper.numEntriesToDelete) +
                                      static_cast<ssize_t>(numEntriesToDelete) -
                                      std::min(numEntries, upper.numEntriesToDelete);

    return entriesAfterMerge >= static_cast<ssize_t>(maxEntries);
}

double LSMTree::insertIntoBufferTree(size_t entries) noexcept(true)
{
    // bufferTree is in RAM (cost = 0)
    double time = 0.0;
    bufferTree.numEntries += entries;

    LOGGER_LOG_TRACE("{} entries inserted into bufferTree, now: ({} + {} = {})/{}, took {}s", entries, bufferTree.numEntries, bufferTree.numEntriesToDelete, bufferTree.numEntries + bufferTree.numEntriesToDelete, bufferTree.maxEntries, 0.0);

    if (bufferTree.isFull())
    {
        if (getHeight() == 0)
            addLevel();

        time += mergeBufferTree();
    }

    return time;
}

double LSMTree::deleteFromBufferTree(size_t entries) noexcept(true)
{
    // bufferTree is in RAM (cost = 0)
    double time = 0.0;
    bufferTree.numEntriesToDelete += entries;

    LOGGER_LOG_TRACE("{} entriesToDelete inserted into bufferTree, now: ({} + {} = {})/{}, took {}s", entries, bufferTree.numEntries, bufferTree.numEntriesToDelete, bufferTree.numEntries + bufferTree.numEntriesToDelete, bufferTree.maxEntries, 0.0);

    if (bufferTree.isFull())
    {
        if (getHeight() == 0)
            addLevel();

        time += mergeBufferTree();
    }

    return time;
}

void LSMTree::addLevel() noexcept(true)
{
    size_t lvlSize = bufferTree.sizeInBytes * lvlRatio;
    if (getHeight() > 0)
        lvlSize = levels[getHeight() - 1].sizeInBytes * lvlRatio;

    LSMLvl lvl{lvlSize, getRecordSize(), getHeight(), nodeSize};
    levels.push_back(lvl);
}

double LSMTree::mergeBufferTree() noexcept(true)
{
    double time = 0.0;

    auto lvl = std::ref(levels[0]);

    LOGGER_LOG_DEBUG("Merging LSM BufferTree ({} + {} = {}) / {} with LSM LVL{} ({} + {} = {}) / {}",
                     bufferTree.numEntries,
                     bufferTree.numEntriesToDelete,
                     bufferTree.numEntries + bufferTree.numEntriesToDelete,
                     bufferTree.maxEntries,
                     lvl.get().getLvl(),
                     lvl.get().numEntries,
                     lvl.get().numEntriesToDelete,
                     lvl.get().numEntries + lvl.get().numEntriesToDelete,
                     lvl.get().maxEntries);

    if (lvl.get().willBeFullAfterMerge(bufferTree))
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
    const size_t entriesAfterMerge = lvl.get().numEntries > bufferTree.numEntriesToDelete ? lvl.get().numEntries - bufferTree.numEntriesToDelete + bufferTree.numEntries : bufferTree.numEntries;

    // how many entries (to delete) will stay in lvl0
    const size_t entriesToDeleteAfterMerge = bufferTree.numEntriesToDelete > lvl.get().numEntries ? bufferTree.numEntriesToDelete - lvl.get().numEntries + lvl.get().numEntriesToDelete : lvl.get().numEntriesToDelete;

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from LSM LVL{} {} bytes, writing into LSM LVL{} {} bytes",
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
    bufferTree.numEntries = 0;
    bufferTree.numEntriesToDelete = 0;

    lvl.get().numEntries = entriesAfterMerge;
    lvl.get().numEntriesToDelete = entriesToDeleteAfterMerge;

    return time;
}

double LSMTree::mergeLevels(size_t upperLvl, size_t lowerLvl) noexcept(true)
{
    double time = 0.0;

    auto upper = std::ref(levels[upperLvl]);
    auto lower = std::ref(levels[lowerLvl]);
    LOGGER_LOG_DEBUG("Merging LSM LVL{} ({} + {} = {}) / {} with LSM LVL{} ({} + {} = {}) / {}",
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

        LOGGER_LOG_DEBUG("LSM LVL{} merged with LSM LVL{} took {}s, now LSM LVL{} ({} + {} = {}) / {}, LSM LVL{} ({} + {} = {}) / {}",
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

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from LSM LVL{} {} bytes, reading from LSM LVL{} {} bytes, writting into LSM LVL{} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     upper.get().getLvl(),
                     (upper.get().numEntries + upper.get().numEntriesToDelete) * getRecordSize(),
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

    // update metadata
    upper.get().numEntries = 0;
    upper.get().numEntriesToDelete = 0;

    lower.get().numEntries = entriesAfterMerge;
    lower.get().numEntriesToDelete = entriesToDeleteAfterMerge;

    return time;
}

double LSMTree::addEntriesToLevel(size_t lvl, size_t entries) noexcept(true)
{
    double time = 0.0;
    auto lsmLvl = std::ref(levels[lvl]);

    if (lsmLvl.get().numEntries + lsmLvl.get().numEntriesToDelete + entries >= lsmLvl.get().maxEntries)
    {
        LOGGER_LOG_ERROR("You did something wrong with lvl {}, ({} + {} + {}) >= {}", lvl, lsmLvl.get().numEntries, lsmLvl.get().numEntriesToDelete, entries, lsmLvl.get().maxEntries);
        return 0.0;
    }

    const size_t entriesAfterMerge = lsmLvl.get().numEntries + entries;
    const size_t entriesToDeleteAfterMerge = lsmLvl.get().numEntriesToDelete;
    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from LSM LVL{} {} bytes, writting into LSM LVL{} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     lsmLvl.get().getLvl(),
                     (lsmLvl.get().numEntries + lsmLvl.get().numEntriesToDelete) * getRecordSize(),
                     lsmLvl.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());

    // read entries from lvl
    uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->readBytes(addr, (lsmLvl.get().numEntries + lsmLvl.get().numEntriesToDelete) * getRecordSize());
    time += disk->flushCache();

    // write entries to lvl
    addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());
    time += disk->flushCache();

    // update metadata
    lsmLvl.get().numEntries = entriesAfterMerge;
    lsmLvl.get().numEntriesToDelete = entriesToDeleteAfterMerge;

    return time;
}

double LSMTree::insertEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        ++numEntries;

        time += insertIntoBufferTree(1);
    }

    return time;
}

double LSMTree::deleteEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        // cannot delete more
        if (numEntries == 0)
            return time;

        --numEntries;

        time += deleteFromBufferTree(1);
    }

    return time;
}

double LSMTree::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    if (getHeight() == 0)
    {
        LOGGER_LOG_TRACE("FindEntries in BufferTree, cost 0.0");

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

            LOGGER_LOG_TRACE("Seek LSM LVL{} took {}s", j + 1, lvlTime);
            time += lvlTime;
        }

    return time;
}

double LSMTree::bulkloadEntriesHelperCurrentCapacity(size_t numEntries) noexcept(true)
{
    double time = 0.0;

    if (numEntries < bufferTree.maxEntries)
    {
        LOGGER_LOG_DEBUG("BulkloadCurrentCapacity({}): numEntries {} < bufferTree.maxEntries {}, chose normal insert", numEntries, numEntries, bufferTree.maxEntries);
        return insertEntriesHelper(numEntries);
    }

    // we can notice it now, no need to wait and branch out this
    this->numEntries += numEntries;

    // if (bufferTree.numEntries + bufferTree.numEntriesToDelete + numEntries < bufferTree.maxEntries)
    // {
    //     LOGGER_LOG_DEBUG("BulkloadCurrentCapacity({}): Entries {} added into bufferTree, took 0s now bufferTree {}", numEntries, numEntries, bufferTree.toStringFull());
    //     bufferTree.numEntries += numEntries;

    //     return 0.0;
    // }

    // find good level in existing levels
    for (size_t i = 0; i < getHeight(); ++i)
    {
        if (levels[i].numEntries + levels[i].numEntriesToDelete + numEntries < levels[i].maxEntries)
        {
            LOGGER_LOG_DEBUG("BulkloadCurrentCapacity({}): Bulkload chose lvl {}", numEntries, levels[i].toStringFull());

            time += addEntriesToLevel(i, numEntries);

            LOGGER_LOG_DEBUG("BulkloadCurrentCapacity({}): Now lvl is: {}", numEntries, levels[i].toStringFull());

            return time;
        }
    }

    // cannot find good level, create levels until we have entries
    while (numEntries > 0)
    {
        LOGGER_LOG_DEBUG("BulkloadCurrentCapacity({}): Create level during bulkload", numEntries);
        addLevel();
        const size_t entriesToInsert = std::min(numEntries, levels[getHeight() - 1].maxEntries - 1);
        time += addEntriesToLevel(getHeight() - 1, entriesToInsert);

        numEntries -= entriesToInsert;
    }

    return time;
}

double LSMTree::bulkloadEntriesHelperMaxCapacity(size_t numEntries) noexcept(true)
{
    double time = 0.0;

    if (numEntries < bufferTree.maxEntries)
    {
        LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): numEntries {} < bufferTree.maxEntries {}, chose normal insert", numEntries, numEntries, bufferTree.maxEntries);
        return insertEntriesHelper(numEntries);
    }

    // we can notice it now, no need to wait and branch out this
    this->numEntries += numEntries;

    // if (bufferTree.numEntries + bufferTree.numEntriesToDelete + numEntries < bufferTree.maxEntries)
    // {
    //     LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): Entries {} added into bufferTree, took 0s now bufferTree {}", numEntries, numEntries, bufferTree.toStringFull());
    //     bufferTree.numEntries += numEntries;

    //     return 0.0;
    // }

    // find good level in existing levels
    for (size_t i = 0; i < getHeight(); ++i)
    {
        if (numEntries < levels[i].maxEntries)
        {
            LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): Bulkload chose lvl {}", numEntries, levels[i].toStringFull());

            // new entries wont fit into lvl, merge with lower
            if (levels[i].numEntries + levels[i].numEntriesToDelete + numEntries >= levels[i].maxEntries)
            {
                LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): lvl {} needs merge with lower", numEntries, i);

                // there is no lower, create lower
                if (i == getHeight() - 1)
                {
                    LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): lvl {} is last lvl create another one", numEntries, i);
                    addLevel();
                }

                time += mergeLevels(i, i + 1);
            }

            // we can add new entries to chosen lvl now
            time += addEntriesToLevel(i, numEntries);

            LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): Now lvl is: {}", numEntries, levels[i].toStringFull());

            return time;
        }
    }

    // cannot find good level, create levels until we have entries
    while (numEntries > 0)
    {
        LOGGER_LOG_DEBUG("BulkloadMaxCapacity({}): Create level during bulkload", numEntries);
        addLevel();
        const size_t entriesToInsert = std::min(numEntries, levels[getHeight() - 1].maxEntries - 1);
        time += addEntriesToLevel(getHeight() - 1, entriesToInsert);

        numEntries -= entriesToInsert;
    }

    return time;
}

LSMTree::LSMTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t bufferTreeSize, size_t lvlRatio, enum BulkloadFeatureMode bulkloadMode)
: DBIndex("LSMTree", disk, sizeKey, sizeData), nodeSize{nodeSize}, lvlRatio{lvlRatio}, bufferTree{LSMLvl(bufferTreeSize, sizeKey + sizeData, -1, nodeSize)}, bulkloadMode{bulkloadMode}
{
    LOGGER_LOG_DEBUG("LSMTree created {}", toStringFull());
}

LSMTree::LSMTree(Disk* disk, size_t sizeKey, size_t sizeData, enum BulkloadFeatureMode bulkloadMode)
: LSMTree(disk, sizeKey, sizeData, 1UL << 21 /* 2MB is a default for LSMTree */, 1UL << 21, 5, bulkloadMode)
{

}

std::string LSMTree::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("LSMTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .bulkloadMode = ") + std::to_string(bulkloadMode) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("LSMTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.bulkloadMode = ") + std::to_string(bulkloadMode) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string LSMTree::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringLSMLvls = [](const std::string &accumulator, const LSMTree::LSMLvl &lvl)
    {
        return accumulator.empty() ? lvl.toStringFull() : accumulator + "," + lvl.toStringFull();
    };

    const std::string fdLvlsString =  std::string("{") +
                                      std::accumulate(std::begin(levels), std::end(levels), std::string(), buildStringLSMLvls) +
                                      std::string("}");


    if (oneLine)
        return std::string(std::string("LSMTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .bulkloadMode = ") + std::to_string(bulkloadMode) +
                           std::string(" .bufferTree = ") + bufferTree.toStringFull() +
                           std::string(" .levels = ") + fdLvlsString +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("LSMTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.bulkloadMode = ") + std::to_string(bulkloadMode) + std::string("\n") +
                           std::string("\t.bufferTree = ") + bufferTree.toStringFull() + std::string("\n") +
                           std::string("\t.levels = ") + fdLvlsString + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

size_t LSMTree::getHeight() const noexcept(true)
{
    return levels.size();
}

const LSMTree::LSMLvl& LSMTree::getLSMLvl(size_t lvl) const noexcept(true)
{
    if (lvl == 0)
        return bufferTree;

    return levels[lvl - 1];
}

bool LSMTree::isBulkloadSupported() const noexcept(true)
{
    return bulkloadMode != BULKLOAD_FEATURE_OFF;
}

double LSMTree::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double LSMTree::bulkloadEntries(size_t numEntries) noexcept(true)
{
    // unsupported
    if (!isBulkloadSupported())
    {
        LOGGER_LOG_WARN("Bulkload is unsupported");

        return 0.0;
    }

    double time = 0.0;

    switch (bulkloadMode)
    {
        case BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY:
        {
            time += bulkloadEntriesHelperCurrentCapacity(numEntries);
            break;
        }
        case BULKLOAD_ACCORDING_TO_MAX_CAPACITY:
        {
            time += bulkloadEntriesHelperMaxCapacity(numEntries);
            break;
        }
        default:
            LOGGER_LOG_ERROR("Something went wrong, bulkloadMode {} unsupported", bulkloadMode);
    }

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Inserted using bulkload {} entries, took {}s", numEntries, time);

    return time;
}

double LSMTree::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double LSMTree::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double LSMTree::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double LSMTree::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double LSMTree::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}