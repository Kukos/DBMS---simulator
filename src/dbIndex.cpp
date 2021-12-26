#include <index/dbIndex.hpp>

std::string DBIndex::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DBIndex {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndex {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toString() + std::string("\n") +
                           std::string("}"));
}

std::string DBIndex::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DBIndex {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .sizeKey = ") + std::to_string(sizeKey) +
                           std::string(" .sizeData = ") + std::to_string(sizeData) +
                           std::string(" .sizeRecord = ") + std::to_string(sizeRecord) +
                           std::string(" .numEntries = ") + std::to_string(numEntries) +
                           std::string(" .disk = ") + disk->toStringFull() +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DBIndex {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.sizeKey = ") + std::to_string(sizeKey) + std::string("\n") +
                           std::string("\t.sizeData = ") + std::to_string(sizeData) + std::string("\n") +
                           std::string("\t.sizeRecord = ") + std::to_string(sizeRecord) + std::string("\n") +
                           std::string("\t.numEntries = ") + std::to_string(numEntries) + std::string("\n") +
                           std::string("\t.disk = ") + disk->toStringFull() + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

void DBIndex::resetState() noexcept(true)
{
    disk->resetState();
    counters = IndexCounters();
}

DBIndex::DBIndex(const char*name, Disk* disk, size_t sizeKey, size_t sizeData)
: name{name}, disk{std::unique_ptr<Disk>(disk)}, sizeKey{sizeKey}, sizeData{sizeData}, sizeRecord{sizeKey + sizeData}, numEntries{0}
{
    LOGGER_LOG_DEBUG("DBIndex created {}", toStringFull());
}

DBIndex::DBIndex(const DBIndex& other)
: DBIndex(other.name, (*other.disk).clone(), other.sizeKey, other.sizeData)
{
    numEntries = other.numEntries;
    counters = other.counters;
}

DBIndex& DBIndex::operator=(const DBIndex& other)
{
    if (this == &other)
        return *this;

    name = other.name;
    sizeKey = other.sizeKey;
    sizeData = other.sizeData;
    sizeRecord = other.sizeRecord;
    numEntries = other.numEntries;
    counters = other.counters;
    disk.reset((*other.disk).clone());

    return *this;
}