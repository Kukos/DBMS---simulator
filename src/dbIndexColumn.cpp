#include <numeric>
#include <index/dbIndexColumn.hpp>

DBIndexColumn::DBIndexColumn(const char*name, Disk* disk, const std::vector<size_t>& columnsSize)
: name{name}, disk{std::unique_ptr<Disk>(disk)}, columnsSize{std::vector<size_t>(columnsSize)}, numEntries{0}
{
    const size_t numOfColumns = columnsSize.size();
    if (numOfColumns < 2)
        LOGGER_LOG_ERROR("NumOfColumns should be at leats 2, got {}", numOfColumns);

    sizeKey = columnsSize[0];

    sizeData = 0;
    for (size_t i = 1; i < numOfColumns; ++i)
        sizeData += columnsSize[i];

    sizeRecord = sizeKey + sizeData;

    LOGGER_LOG_DEBUG("DBIndexColumn created {}", toStringFull());
}

DBIndexColumn::DBIndexColumn(const DBIndexColumn& other)
: DBIndexColumn(other.name, (*other.disk).clone(), other.columnsSize)
{
    numEntries = other.numEntries;
    counters = other.counters;
}

DBIndexColumn& DBIndexColumn::operator=(const DBIndexColumn& other)
{
    if (this == &other)
        return *this;

    name = other.name;
    sizeKey = other.sizeKey;
    sizeData = other.sizeData;
    sizeRecord = other.sizeRecord;
    disk.reset((*other.disk).clone());

    numEntries = other.numEntries;
    columnsSize = other.columnsSize;
    counters = other.counters;

    return *this;
}

std::string DBIndexColumn::toString(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");


    if (oneLine)
        return std::string(std::string("DBIndexColumn {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndexColumn {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string DBIndexColumn::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");

    if (oneLine)
        return std::string(std::string("DBIndexColumn {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndexColumn {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}