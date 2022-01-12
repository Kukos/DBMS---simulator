#include <index/cfdtree.hpp>
#include <logger/logger.hpp>

#include <numeric>

CFDTree::CFDTree(const char* name, Disk* disk, const std::vector<size_t>& columnsSize, size_t nodeSize, size_t headTreeSize, size_t lvlRatio)
: DBIndexColumn(name, disk, columnsSize), disk{std::unique_ptr<DiskColumnOverlay>(new DiskColumnOverlay(disk))}, nodeSize{nodeSize}, headTreeSize{headTreeSize}, lvlRatio{lvlRatio}
{
    const size_t maxEntries = nodeSize / columnsSize[0];
    const size_t headTreeMultipler = headTreeSize / nodeSize;

    fdColumns.reserve(columnsSize.size());
    for (size_t i = 0; i < columnsSize.size(); ++i)
    {
        const size_t cNodeSize = i == 0 ? (maxEntries * (columnsSize[i])) : (maxEntries * (columnsSize[i] + columnsSize[0]));
        fdColumns.push_back(FDTree(new Disk(*disk), columnsSize[0], i == 0 ? 0 : columnsSize[i], nodeSize, cNodeSize * headTreeMultipler, lvlRatio));
    }

    LOGGER_LOG_DEBUG("CFDTree created {}", toStringFull());
}


CFDTree::CFDTree(Disk* disk, const std::vector<size_t>& columnsSize, size_t nodeSize, size_t headTreeSize, size_t lvlRatio)
: CFDTree("CFDTree", disk, columnsSize, nodeSize, headTreeSize, lvlRatio)
{

}

CFDTree::CFDTree(Disk* disk, const std::vector<size_t>& columnsSize)
: CFDTree(disk, columnsSize, disk->getLowLevelController().getPageSize(), disk->getLowLevelController().getPageSize(), 10)
{

}

CFDTree::CFDTree(const char* name, Disk* disk, const std::vector<size_t>& columnsSize)
: CFDTree(name, disk, columnsSize, disk->getLowLevelController().getPageSize(), disk->getLowLevelController().getPageSize(), 10)
{

}

CFDTree::CFDTree(const CFDTree& other)
: CFDTree(other.name, (*other.disk).clone(), other.columnsSize, other.nodeSize, other.headTreeSize, other.lvlRatio)
{

}

CFDTree& CFDTree::operator=(const CFDTree& other)
{
    if (this == &other)
        return *this;

    DBIndexColumn::operator =(other);
    disk.reset(new DiskColumnOverlay(other.disk.get()));
    fdColumns = other.fdColumns;
    nodeSize = other.nodeSize;
    headTreeSize = other.headTreeSize;
    lvlRatio = other.lvlRatio;

    return *this;
}

std::string CFDTree::toString(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");

    (void)getDisk(); // this triggers disk stat merge

    if (oneLine)
        return std::string(std::string("CFDTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("CFDTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("}"));
}

std::string CFDTree::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");

    auto buildStringFDColumns = [](const std::string &accumulator, const FDTree &fd)
    {
        return accumulator.empty() ? fd.toStringFull() : accumulator + "," + fd.toStringFull();
    };

    const std::string fdColumnsString =  std::string("{") +
                                         std::accumulate(std::begin(fdColumns), std::end(fdColumns), std::string(), buildStringFDColumns) +
                                         std::string("}");

    (void)getDisk(); // this triggers disk stat merge

    IndexCounters aggrCounters;
    for (enum IndexCounters::IndexCountersL c = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS; c <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS; ++c)
        aggrCounters.pegCounter(c, getCounter(c).second);

    for (enum IndexCounters::IndexCountersD c = IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME; c <= IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME; ++c)
        aggrCounters.pegCounter(c, getCounter(c).second);

    if (oneLine)
        return std::string(std::string("CFDTree {") +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + aggrCounters.toStringFull() +
                           std::string(" .fdColumns = ") + fdColumnsString +
                           std::string(" }"));
    else
        return std::string(std::string("CFDTree {\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + aggrCounters.toStringFull() + std::string("\n") +
                           std::string("\t.fdColumns = ") + fdColumnsString + std::string("\n") +
                           std::string("}"));
}

const Disk& CFDTree::getDisk() const noexcept(true)
{
    // We need to merge all pieces of disks to 1 disk
    disk->resetState();

    for (size_t i = 0; i < fdColumns.size(); ++i)
        dynamic_cast<DiskColumnOverlay*>(disk.get())->addStats(fdColumns[i].getDisk());

    return *disk.get();
}

std::pair<std::string, double> CFDTree::getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true)
{
    double val = 0;
    for (size_t i = 0; i < fdColumns.size(); ++i)
        val += fdColumns[i].getCounter(counterId).second;

    return std::pair<std::string, double>(fdColumns[0].getCounter(counterId).first, val);
}

std::pair<std::string, long> CFDTree::getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true)
{
    return fdColumns[0].getCounter(counterId);
}

void CFDTree::resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true)
{
    for (size_t i = 0; i < fdColumns.size(); ++i)
        fdColumns[i].resetCounter(counterId);
}

void CFDTree::resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true)
{
    for (size_t i = 0; i < fdColumns.size(); ++i)
        fdColumns[i].resetCounter(counterId);
}

void CFDTree::resetAllCounters() noexcept(true)
{
    for (size_t i = 0; i < fdColumns.size(); ++i)
        fdColumns[i].resetAllCounters();
}

size_t CFDTree::getNumEntries() const noexcept(true)
{
    return fdColumns[0].getNumEntries();
}

size_t CFDTree::getHeight() const noexcept(true)
{
    return fdColumns[0].getHeight();
}

const FDTree::FDLvl& CFDTree::getCFDLvl(size_t lvl, size_t columnIndex) const noexcept(true)
{
    if (columnIndex >= fdColumns.size())
        LOGGER_LOG_ERROR("ColumnIndex = {} is greater than number of columns {}", columnIndex, fdColumns.size());

    return fdColumns[columnIndex].getFDLvl(lvl);
}

bool CFDTree::isBulkloadSupported() const noexcept(true)
{
    return false;
}

double CFDTree::insertEntries(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < fdColumns.size(); ++i)
        time += fdColumns[i].insertEntries(numOperations);

    return time;
}

double CFDTree::bulkloadEntries(size_t numEntries) noexcept(true)
{
    (void)numEntries;

    LOGGER_LOG_WARN("Bulkload is unsupported");

    // unsupported
    return 0.0;
}

double CFDTree::deleteEntries(size_t numOperations) noexcept(true)
{
    double time = 0.0;

    for (size_t i = 0; i < fdColumns.size(); ++i)
        time += fdColumns[i].deleteEntries(numOperations);

    return time;
}

double CFDTree::findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    std::vector colCopy = columnsToFetch;
    std::sort(colCopy.begin(), colCopy.end());

    if (colCopy[0] != 0)
    {
        LOGGER_LOG_ERROR("You need key column!");
        return 0.0;
    }

    for (size_t i = 0; i < colCopy.size(); ++i)
        time += fdColumns[colCopy[i]].findPointEntries(numOperations);

    return time;
}

double CFDTree::findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    std::vector colCopy = columnsToFetch;
    std::sort(colCopy.begin(), colCopy.end());

    if (colCopy[0] != 0)
    {
        LOGGER_LOG_ERROR("You need key column!");
        return 0.0;
    }

    for (size_t i = 0; i < colCopy.size(); ++i)
        time += fdColumns[colCopy[i]].findPointEntries(selectivity, numOperations);

    return time;
}

double CFDTree::findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations)  noexcept(true)
{
    double time = 0.0;

    std::vector colCopy = columnsToFetch;
    std::sort(colCopy.begin(), colCopy.end());

    if (colCopy[0] != 0)
    {
        LOGGER_LOG_ERROR("You need key column!");
        return 0.0;
    }

    for (size_t i = 0; i < colCopy.size(); ++i)
        time += fdColumns[colCopy[i]].findRangeEntries(numEntries, numOperations);

    return time;
}

double CFDTree::findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    double time = 0.0;

    std::vector colCopy = columnsToFetch;
    std::sort(colCopy.begin(), colCopy.end());

    if (colCopy[0] != 0)
    {
        LOGGER_LOG_ERROR("You need key column!");
        return 0.0;
    }

    for (size_t i = 0; i < colCopy.size(); ++i)
        time += fdColumns[colCopy[i]].findRangeEntries(selectivity, numOperations);

    return time;
}

void CFDTree::createTopologyAfterInsert(size_t numEntries) noexcept(true)
{
    for (size_t i = 0; i < fdColumns.size(); ++i)
        fdColumns[i].createTopologyAfterInsert(numEntries);
}