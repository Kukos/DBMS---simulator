#include <disk/disk.hpp>
#include <logger/logger.hpp>

const MemoryController& Disk::getLowLevelController() const noexcept(true)
{
    return *memoryController.get();
}

uintptr_t Disk::getCurrentMemoryAddr() const noexcept(true)
{
    return memoryController->getCurrentMemoryAddr();
}

double Disk::flushCache() noexcept(true)
{
    // We want to peg counters like in controller,
    // so get values before flushing and peg counter to align with controller counters
    const double writeTimeCounter = memoryController->getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second;
    const double overwriteTimeCounter = memoryController->getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second;

    const double flushTime = memoryController->flushCache();

    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, memoryController->getCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME).second - writeTimeCounter);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, memoryController->getCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME).second - overwriteTimeCounter);

    return flushTime;
}

double Disk::readBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    const double time = memoryController->readBytes(addr, bytes);

    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME, time);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, 1);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES, bytes);

    return time;
}

double Disk::writeBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    const double time = memoryController->writeBytes(addr, bytes);

    // on most of the disks (right now on every disk) time is 0, because real writing is during flushing the cache
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, time);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, 1);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, bytes);

    return time;
}

double Disk::overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true)
{
    const double time = memoryController->overwriteBytes(addr, bytes);

    // on most of the disks (right now on every disk) time is 0, because real overwriting is during flushing the cache
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, time);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, 1);
    diskCounters.pegCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, bytes);

    return time;
}

std::pair<std::string, double> Disk::getDiskCounter(enum MemoryCounters::MemoryCountersD counterId) const noexcept(true)
{
    return diskCounters.getCounter(counterId);
}

std::pair<std::string, long> Disk::getDiskCounter(enum MemoryCounters::MemoryCountersL counterId) const noexcept(true)
{
    return diskCounters.getCounter(counterId);
}

void Disk::resetDiskCounter(enum MemoryCounters::MemoryCountersL counterId) noexcept(true)
{
    diskCounters.resetCounter(counterId);
}

void Disk::resetDiskCounter(enum MemoryCounters::MemoryCountersD counterId) noexcept(true)
{
    diskCounters.resetCounter(counterId);
}

void Disk::resetAllDiskCounters() noexcept(true)
{
    diskCounters.resetAllCounters();
}

std::string Disk::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("Disk {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("Disk {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}


std::string Disk::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("Disk {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("Disk {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}

Disk::Disk(MemoryController* controller)
: memoryController{std::unique_ptr<MemoryController>(controller)}
{
    LOGGER_LOG_DEBUG("Disk created: {}", toStringFull());
}