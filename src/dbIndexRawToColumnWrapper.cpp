#include <index/dbIndexRawToColumnWrapper.hpp>
#include <logger/logger.hpp>
#include <numeric>

DBIndexRawToColumnWrapper::DBIndexRawToColumnWrapper(DBIndex* rawIndex, const std::vector<size_t>& columnsSize)
: DBIndexColumn(rawIndex->getName(), new Disk(rawIndex->getDisk()), columnsSize), rawIndex{std::unique_ptr<DBIndex>(rawIndex)}
{
    LOGGER_LOG_DEBUG("DBIndexRawToColumnWrapper created {}", toStringFull());
}

std::string DBIndexRawToColumnWrapper::toString(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");


    if (oneLine)
        return std::string(std::string("DBIndexRawToColumnWrapper {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .rawIndex = ") + rawIndex->toString() +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndexRawToColumnWrapper {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.rawIndex = ") + rawIndex->toString() + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string DBIndexRawToColumnWrapper::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnsSize), std::end(columnsSize), std::string(), buildStringFromVector) + std::string(" }");

    if (oneLine)
        return std::string(std::string("DBIndexRawToColumnWrapper {") +\
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .rawIndex = ") + rawIndex->toStringFull() +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndexRawToColumnWrapper {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.rawIndex = ") + rawIndex->toStringFull() + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

size_t DBIndexRawToColumnWrapper::getNumEntries() const noexcept(true)
{
    return rawIndex->getNumEntries();
}

bool DBIndexRawToColumnWrapper::isBulkloadSupported() const noexcept(true)
{
    return rawIndex->isBulkloadSupported();
}

double DBIndexRawToColumnWrapper::insertEntries(size_t numOperations) noexcept(true)
{
    return rawIndex->insertEntries(numOperations);
}

double DBIndexRawToColumnWrapper::bulkloadEntries(size_t numEntries) noexcept(true)
{
    return rawIndex->bulkloadEntries(numEntries);
}

double DBIndexRawToColumnWrapper::deleteEntries(size_t numOperations) noexcept(true)
{
    return rawIndex->deleteEntries(numOperations);
}

double DBIndexRawToColumnWrapper::findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations)  noexcept(true)
{
    (void)columnsToFetch;
    return rawIndex->findPointEntries(numOperations);
}

double DBIndexRawToColumnWrapper::findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    (void)columnsToFetch;
    return rawIndex->findPointEntries(selectivity, numOperations);
}

double DBIndexRawToColumnWrapper::findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations)  noexcept(true)
{
    (void)columnsToFetch;
    return rawIndex->findRangeEntries(numEntries, numOperations);
}

double DBIndexRawToColumnWrapper::findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations) noexcept(true)
{
    (void)columnsToFetch;
    return rawIndex->findRangeEntries(selectivity, numOperations);
}

DBIndexRawToColumnWrapper::DBIndexRawToColumnWrapper(const DBIndexRawToColumnWrapper& other)
: DBIndexRawToColumnWrapper((*other.rawIndex).clone(), other.columnsSize)
{

}

DBIndexRawToColumnWrapper& DBIndexRawToColumnWrapper::operator=(const DBIndexRawToColumnWrapper& other)
{
    if (this == &other)
        return *this;

    DBIndexColumn::operator =(other);
    rawIndex.reset((*other.rawIndex).clone());

    return *this;
}