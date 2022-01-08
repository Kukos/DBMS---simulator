#include <table/dbTable.hpp>
#include <logger/logger.hpp>

#include <numeric>

DBTable::DBTable(const char* name, std::vector<size_t> columnSize)
: name{name}, columnSize{columnSize}
{
    if (columnSize.size() == 0)
    {
        LOGGER_LOG_ERROR("Table needs at least 1 columns");
        return;
    }

    sizeKey = columnSize[0];
    sizeData = 0;
    for (size_t i = 1; i < columnSize.size(); ++i)
        sizeData += columnSize[i];

    sizeRecord = sizeKey + sizeData;

    LOGGER_LOG_DEBUG("DBTable created {}", toStringFull());
}

DBTable::DBTable(std::vector<size_t> columnSize)
: DBTable("DBTable", columnSize)
{

}

std::string DBTable::toString(bool oneLine) const noexcept(true)
{
    return toStringFull(oneLine);
}

std::string DBTable::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromVector = [](const std::string &accumulator, const size_t &columnSize)
    {
        return accumulator.empty() ? std::to_string(columnSize) : accumulator + "," + std::to_string(columnSize);
    };

    const std::string columnsString = std::string("{ ") + std::accumulate(std::begin(columnSize), std::end(columnSize), std::string(), buildStringFromVector) + std::string(" }");


    if (oneLine)
        return std::string(std::string("DBTable {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .columnsSize = ") + columnsString +
                           std::string(" }"));
    else
        return std::string(std::string("DBTable {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.columnsSize = ") + columnsString + std::string("\n") +
                           std::string("}"));
}