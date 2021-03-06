#include <index/falsmtree.hpp>
#include <logger/logger.hpp>
#include <numeric>

FALSMTree::FALSMLvl::FALSMLvl(size_t sizeInBytes, size_t recordSize, size_t lvl, size_t nodeSize)
: sizeInBytes{sizeInBytes}, sizeInNodes{(sizeInBytes + nodeSize - 1)/ nodeSize},  numEntries{0}, numEntriesToDelete{0}, maxEntries{sizeInBytes / recordSize}, lvl{lvl}, numOverflowNodes{0}
{
    if (lvl == static_cast<size_t>(-1))
        LOGGER_LOG_TRACE("FALSM BufferTree created {}", toStringFull());
    else
        LOGGER_LOG_TRACE("FALSM Lvl {} created {}", lvl, toStringFull());
}

std::string FALSMTree::FALSMLvl::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("FALSMLvl {") +
                           std::string(" .lvl = ") + std::to_string(lvl) +
                           std::string(" .sizeInBytes = ") + std::to_string(sizeInBytes) +
                           std::string(" .sizeInNodes = ") + std::to_string(sizeInNodes) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .numEntriesToDelete = ") + std::to_string(numEntriesToDelete) +
                           std::string(" .maxEntries = ") + std::to_string(maxEntries) +
                           std::string(" .numOverflowNodes = ") + std::to_string(numOverflowNodes) +
                           std::string(" }"));
    else
        return std::string(std::string("FALSMLvl {\n") +
                           std::string("\t.lvl = ") + std::to_string(lvl) + std::string("\n") +
                           std::string("\t.sizeInBytes = ") + std::to_string(sizeInBytes) + std::string("\n") +
                           std::string("\t.sizeInNodes = ") + std::to_string(sizeInNodes) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.numEntriesToDelete = ") + std::to_string(numEntriesToDelete) + std::string("\n") +
                           std::string("\t.maxEntries = ") + std::to_string(maxEntries) + std::string("\n") +
                           std::string("\t.numOverflowNodes = ") + std::to_string(numOverflowNodes) + std::string("\n") +
                           std::string("}"));
}

std::string FALSMTree::FALSMLvl::toStringFull(bool oneLine) const noexcept(true)
{
    return toString(oneLine);
}

bool FALSMTree::FALSMLvl::isFull() const noexcept(true)
{
    return numEntries + numEntriesToDelete >= maxEntries;
}

bool FALSMTree::FALSMLvl::willBeFullAfterMerge(const FALSMLvl& upper)
{
    const ssize_t entriesAfterMerge = static_cast<ssize_t>(numEntries) +
                                      static_cast<ssize_t>(upper.numEntries) +
                                      static_cast<ssize_t>(upper.numEntriesToDelete) +
                                      static_cast<ssize_t>(numEntriesToDelete) -
                                      std::min(numEntries, upper.numEntriesToDelete);

    return entriesAfterMerge >= static_cast<ssize_t>(maxEntries);
}

double FALSMTree::insertIntoBufferTree(size_t entries) noexcept(true)
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

double FALSMTree::deleteFromBufferTree(size_t entries) noexcept(true)
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

void FALSMTree::addLevel() noexcept(true)
{
    size_t lvlSize = bufferTree.sizeInBytes * lvlRatio;
    if (getHeight() > 0)
        lvlSize = levels[getHeight() - 1].sizeInBytes * lvlRatio;

    FALSMLvl lvl{lvlSize, getRecordSize(), getHeight(), nodeSize};
    levels.push_back(lvl);
}

double FALSMTree::mergeBufferTree() noexcept(true)
{
    double time = 0.0;

    auto lvl = std::ref(levels[0]);

    LOGGER_LOG_DEBUG("Merging FALSM BufferTree ({} + {} = {}) / {} with FALSM LVL{} ({} + {} = {}) / {}",
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

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FALSM LVL{} {} bytes, writing into FALSM LVL{} {} bytes",
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
    lvl.get().numOverflowNodes = 0;

    return time;
}

double FALSMTree::mergeBufferTreeFake() noexcept(true)
{
    double time = 0.0;

    auto lvl = std::ref(levels[0]);

    LOGGER_LOG_DEBUG("FAKE Merging FALSM BufferTree ({} + {} = {}) / {} with FALSM LVL{} ({} + {} = {}) / {}",
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

        const double mergeTime = mergeLevelsFake(0, 1);
        lvl = std::ref(levels[0]);
        auto lower = std::ref(levels[1]);

        LOGGER_LOG_DEBUG("LVL{} FAKE merged with LVL{} took {}s, now LVL{} ({} + {} = {}) / {}, LVL{} ({} + {} = {}) / {}",
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

    LOGGER_LOG_TRACE("FAKE Merge buffer: entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FALSM LVL{} {} bytes, writing into FALSM LVL{} {} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     lvl.get().getLvl(),
                     (lvl.get().numEntries + lvl.get().numEntriesToDelete) * getRecordSize(),
                     lvl.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());

    // update metadata
    bufferTree.numEntries = 0;
    bufferTree.numEntriesToDelete = 0;

    lvl.get().numEntries = entriesAfterMerge;
    lvl.get().numEntriesToDelete = entriesToDeleteAfterMerge;
    lvl.get().numOverflowNodes = 0;

    return time;
}


double FALSMTree::mergeLevels(size_t upperLvl, size_t lowerLvl) noexcept(true)
{
    double time = 0.0;

    auto upper = std::ref(levels[upperLvl]);
    auto lower = std::ref(levels[lowerLvl]);
    LOGGER_LOG_DEBUG("Merging FALSM LVL{} ({} + {} = {}) / {} with FALSM LVL{} ({} + {} = {}) / {}",
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

        LOGGER_LOG_DEBUG("FALSM LVL{} merged with FALSM LVL{} took {}s, now FALSM LVL{} ({} + {} = {}) / {}, FALSM LVL{} ({} + {} = {}) / {}",
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

    LOGGER_LOG_TRACE("entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FALSM LVL{} {} bytes, reading from FALSM LVL{} {} bytes, writting into FALSM LVL{} bytes",
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
    upper.get().numOverflowNodes = 0;

    lower.get().numEntries = entriesAfterMerge;
    lower.get().numEntriesToDelete = entriesToDeleteAfterMerge;
    lower.get().numOverflowNodes = 0;

    return time;
}

double FALSMTree::mergeLevelsFake(size_t upperLvl, size_t lowerLvl) noexcept(true)
{
    double time = 0.0;

    auto upper = std::ref(levels[upperLvl]);
    auto lower = std::ref(levels[lowerLvl]);
    LOGGER_LOG_DEBUG("FAKE: Merging FALSM LVL{} ({} + {} = {}) / {} with FALSM LVL{} ({} + {} = {}) / {}",
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

        const double mergeTime = mergeLevelsFake(lowerLvl, lowerLvl + 1);
        upper = std::ref(levels[upperLvl]);
        lower = std::ref(levels[lowerLvl]);
        auto lowerLow = std::ref(levels[lowerLvl + 1]);

        LOGGER_LOG_DEBUG("FALSM LVL{} FAKE merged with FALSM LVL{} took {}s, now FALSM LVL{} ({} + {} = {}) / {}, FALSM LVL{} ({} + {} = {}) / {}",
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

    LOGGER_LOG_TRACE("FAKE merge: entriesAfterMerge = {}, entriesToDeleteAfterMerge={}, reading from FALSM LVL{} {} bytes, reading from FALSM LVL{} {} bytes, writting into FALSM LVL{} bytes",
                     entriesAfterMerge,
                     entriesToDeleteAfterMerge,
                     upper.get().getLvl(),
                     (upper.get().numEntries + upper.get().numEntriesToDelete) * getRecordSize(),
                     lower.get().getLvl(),
                     (lower.get().numEntries + lower.get().numEntriesToDelete) * getRecordSize(),
                     lower.get().getLvl(),
                     (entriesAfterMerge + entriesToDeleteAfterMerge) * getRecordSize());

    // update metadata
    upper.get().numEntries = 0;
    upper.get().numEntriesToDelete = 0;
    upper.get().numOverflowNodes = 0;

    lower.get().numEntries = entriesAfterMerge;
    lower.get().numEntriesToDelete = entriesToDeleteAfterMerge;
    lower.get().numOverflowNodes = 0;

    return time;
}


double FALSMTree::addEntriesToLevel(size_t lvl, size_t entries) noexcept(true)
{
    double time = 0.0;
    auto falsmLvl = std::ref(levels[lvl]);

    if (falsmLvl.get().numEntries + falsmLvl.get().numEntriesToDelete + entries >= falsmLvl.get().maxEntries)
    {
        LOGGER_LOG_ERROR("You did something wrong with lvl {}, ({} + {} + {}) >= {}", lvl, falsmLvl.get().numEntries, falsmLvl.get().numEntriesToDelete, entries, falsmLvl.get().maxEntries);
        return 0.0;
    }

    const size_t entriesAfterInsert = falsmLvl.get().numEntries + entries;
    const size_t entriesToDeleteAfterInsert = falsmLvl.get().numEntriesToDelete;
    LOGGER_LOG_TRACE("entriesAfterInsert = {}, entriesToDeleteAfterInsert={}, reading from FALSM LVL{} {} bytes, writting into FALSM LVL{} bytes",
                     entriesAfterInsert,
                     entriesToDeleteAfterInsert,
                     falsmLvl.get().getLvl(),
                     0,
                     falsmLvl.get().getLvl(),
                     entries * getRecordSize());

    // write entries to lvl
    uintptr_t addr = disk->getCurrentMemoryAddr();
    time += disk->writeBytes(addr, entries * getRecordSize());
    time += disk->flushCache();

    if (falsmLvl.get().numEntries != 0)
        ++falsmLvl.get().numOverflowNodes;

    // update metadata
    falsmLvl.get().numEntries = entriesAfterInsert;
    falsmLvl.get().numEntriesToDelete = entriesToDeleteAfterInsert;

    return time;
}

double FALSMTree::insertEntriesHelper(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < numOperations; ++i)
    {
        ++numEntries;

        time += insertIntoBufferTree(1);
    }

    return time;
}

double FALSMTree::deleteEntriesHelper(size_t numOperations) noexcept(true)
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

double FALSMTree::findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true)
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

            for (size_t k = 0; k < levels[j].numOverflowNodes + 1; ++k)
            {
                addr = disk->getCurrentMemoryAddr();
                size_t entryIndex = maxEntries / 2;
                while (entryIndex < maxEntries - 1)
                {
                    lvlTime += disk->readBytes(entryIndex * getRecordSize() + addr, sizeKey);
                    entryIndex += (maxEntries - entryIndex) / 2;
                }

                // on last step do not flush cache
                if (k != levels[j].numOverflowNodes + 1 - 1)
                    lvlTime += disk->flushCache();
            }

            if (j == getHeight() - 1)
                lvlTime += disk->readBytes((maxEntries / 2) * getRecordSize() + addr, numEntries == 1 ? sizeData : numEntries * getRecordSize());

            lvlTime += disk->flushCache();

            LOGGER_LOG_TRACE("Seek FALSM LVL{} took {}s", j + 1, lvlTime);
            time += lvlTime;
        }

    return time;
}

double FALSMTree::bulkloadEntriesHelper(size_t numEntries) noexcept(true)
{
    double time = 0.0;

    if (numEntries < bufferTree.maxEntries)
    {
        LOGGER_LOG_DEBUG("Bulkload({}): numEntries {} < bufferTree.maxEntries {}, chose normal insert", numEntries, numEntries, bufferTree.maxEntries);
        return insertEntriesHelper(numEntries);
    }

    // we can notice it now, no need to wait and branch out this
    this->numEntries += numEntries;

    // if (bufferTree.numEntries + bufferTree.numEntriesToDelete + numEntries < bufferTree.maxEntries)
    // {
    //     LOGGER_LOG_DEBUG("Bulkload({}): Entries {} added into bufferTree, took 0s now bufferTree {}", numEntries, numEntries, bufferTree.toStringFull());
    //     bufferTree.numEntries += numEntries;

    //     return 0.0;
    // }

    // find good level in existing levels
    for (size_t i = 0; i < getHeight(); ++i)
    {
        if (numEntries < levels[i].maxEntries / capRatio)
        {
            LOGGER_LOG_DEBUG("Bulkload({}): Bulkload chose lvl {}", numEntries, levels[i].toStringFull());

            // new entries wont fit into lvl, merge with lower
            if (levels[i].numEntries + levels[i].numEntriesToDelete + numEntries >= levels[i].maxEntries)
            {
                LOGGER_LOG_DEBUG("Bulkload({}): lvl {} needs merge with lower", numEntries, i);

                // there is no lower, create lower
                if (i == getHeight() - 1)
                {
                    LOGGER_LOG_DEBUG("Bulkload({}): lvl {} is last lvl create another one", numEntries, i);
                    addLevel();
                }

                time += mergeLevels(i, i + 1);
            }

            // we can add new entries to chosen lvl now
            time += addEntriesToLevel(i, numEntries);

            LOGGER_LOG_DEBUG("Bulkload({}): Now lvl is: {}", numEntries, levels[i].toStringFull());

            return time;
        }
    }

    // cannot find good level, create levels until we have entries
    while (numEntries > 0)
    {
        LOGGER_LOG_DEBUG("Bulkload({}): Create level during bulkload", numEntries);
        addLevel();
        const size_t entriesToInsert = std::min(numEntries, levels[getHeight() - 1].maxEntries - 1);
        time += addEntriesToLevel(getHeight() - 1, entriesToInsert);

        numEntries -= entriesToInsert;
    }

    return time;
}


FALSMTree::FALSMTree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t bufferTreeSize, size_t lvlRatio, size_t capRatio)
: DBIndex(name, disk, sizeKey, sizeData), nodeSize{nodeSize}, lvlRatio{lvlRatio}, capRatio{capRatio}, bufferTree{FALSMLvl(bufferTreeSize, sizeKey + sizeData, -1, nodeSize)}
{
    LOGGER_LOG_DEBUG("FALSMTree created {}", toStringFull());
}

FALSMTree::FALSMTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t bufferTreeSize, size_t lvlRatio, size_t capRatio)
: FALSMTree("FALSMTree", disk, sizeKey, sizeData, nodeSize, bufferTreeSize, lvlRatio, capRatio)
{

}

FALSMTree::FALSMTree(Disk* disk, size_t sizeKey, size_t sizeData)
: FALSMTree(disk, sizeKey, sizeData, 1UL << 21 /* 2MB is a default for FALSMTree */, 1UL << 21, 5)
{

}

FALSMTree::FALSMTree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData)
: FALSMTree(name, disk, sizeKey, sizeData, 1UL << 21 /* 2MB is a default for FALSMTree */, 1UL << 21, 5)
{

}


std::string FALSMTree::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("FALSMTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .capRatio = ") + std::to_string(capRatio) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("FALSMTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.capRatio = ") + std::to_string(capRatio) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string FALSMTree::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFALSMLvls = [](const std::string &accumulator, const FALSMTree::FALSMLvl &lvl)
    {
        return accumulator.empty() ? lvl.toStringFull() : accumulator + "," + lvl.toStringFull();
    };

    const std::string fdLvlsString =  std::string("{") +
                                      std::accumulate(std::begin(levels), std::end(levels), std::string(), buildStringFALSMLvls) +
                                      std::string("}");


    if (oneLine)
        return std::string(std::string("FALSMTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .nodeSize = ") + std::to_string(nodeSize) +
                           std::string(" .lvlRatio = ") + std::to_string(lvlRatio) +
                           std::string(" .capRatio = ") + std::to_string(capRatio) +
                           std::string(" .bufferTree = ") + bufferTree.toStringFull() +
                           std::string(" .levels = ") + fdLvlsString +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("FALSMTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.nodeSize = ") + std::to_string(nodeSize) + std::string("\n") +
                           std::string("\t.lvlRatio = ") + std::to_string(lvlRatio) + std::string("\n") +
                           std::string("\t.capRatio = ") + std::to_string(capRatio) + std::string("\n") +
                           std::string("\t.bufferTree = ") + bufferTree.toStringFull() + std::string("\n") +
                           std::string("\t.levels = ") + fdLvlsString + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

size_t FALSMTree::getHeight() const noexcept(true)
{
    return levels.size();
}

const FALSMTree::FALSMLvl& FALSMTree::getFALSMLvl(size_t lvl) const noexcept(true)
{
    if (lvl == 0)
        return bufferTree;

    return levels[lvl - 1];
}

bool FALSMTree::isBulkloadSupported() const noexcept(true)
{
    return true;
}

double FALSMTree::insertEntries(size_t numOperations) noexcept(true)
{
    const double time = insertEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Inserted {} entries, took {}s", numOperations, time);

    return time;
}

double FALSMTree::bulkloadEntries(size_t numEntries) noexcept(true)
{
    const double time = bulkloadEntriesHelper(numEntries);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, 1);

    LOGGER_LOG_TRACE("Inserted using bulkload {} entries, took {}s", numEntries, time);

    return time;
}

double FALSMTree::deleteEntries(size_t numOperations) noexcept(true)
{
    const size_t realDeletion = std::min(numEntries, numOperations);
    const double time = deleteEntriesHelper(numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, realDeletion);

    LOGGER_LOG_TRACE("Deleted {} entries, took {}s", numOperations, time);

    return time;
}

double FALSMTree::findPointEntries(size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(1, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double FALSMTree::findPointEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findPointEntries({})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);

    return findPointEntries(static_cast<size_t>(static_cast<double>(numEntries) * selectivity) * numOperations);
}

double FALSMTree::findRangeEntries(size_t numEntries, size_t numOperations) noexcept(true)
{
    const double time = findEntriesHelper(numEntries, numOperations);

    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, time);
    counters.pegCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, numOperations);

    LOGGER_LOG_TRACE("Found {} entries, took {}s", numOperations, time);

    return time;
}

double FALSMTree::findRangeEntries(double selectivity, size_t numOperations) noexcept(true)
{
    LOGGER_LOG_TRACE("selectivity={}, numOperations={}, call findRangeEntries({}, {})", selectivity, numOperations, static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);

    return findRangeEntries(static_cast<size_t>(static_cast<double>(this->numEntries) * selectivity), numOperations);
}

void FALSMTree::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    LOGGER_LOG_DEBUG("Creating a topology now entries={}, inserting new {} entries, after we will have {} entries", this->numEntries, numEntries, this->numEntries + numEntries);

    while (numEntries > 0)
    {
        const size_t entriesToInsert = std::min(numEntries, bufferTree.maxEntries - (bufferTree.numEntries + bufferTree.numEntriesToDelete));

        this->numEntries += entriesToInsert;
        bufferTree.numEntries += entriesToInsert;
        if (bufferTree.isFull())
        {
            if (getHeight() == 0)
                addLevel();

            (void)mergeBufferTreeFake();
        }

        numEntries -= entriesToInsert;
    }
}