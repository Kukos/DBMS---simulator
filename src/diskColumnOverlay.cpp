#include <disk/diskColumnOverlay.hpp>
#include <storage/memoryControllerColumnOverlay.hpp>
#include <storage/memoryModelColumnOverlay.hpp>
#include <logger/logger.hpp>

DiskColumnOverlay::DiskColumnOverlay(MemoryController* controller)
: Disk(new MemoryControllerColumnOverlay(controller))
{
    LOGGER_LOG_DEBUG("Disk Column Overlay created from controller: {}", toStringFull());
}

DiskColumnOverlay::DiskColumnOverlay(Disk* disk)
: Disk(*disk)
{
    memoryController.reset(new MemoryControllerColumnOverlay(&const_cast<MemoryController&>(disk->getLowLevelController())));

    for (enum MemoryCounters::MemoryCountersD counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++counterId)
        setCounter(counterId, disk->getDiskCounter(counterId).second);

    for (enum MemoryCounters::MemoryCountersL counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++counterId)
        setCounter(counterId, disk->getDiskCounter(counterId).second);

    LOGGER_LOG_DEBUG("Disk Column Overlay created from disk: {}", toStringFull());
}

void DiskColumnOverlay::pegCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true)
{
    diskCounters.pegCounter(counterId, value);
}

void DiskColumnOverlay::pegCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true)
{
    diskCounters.pegCounter(counterId, value);
}

void DiskColumnOverlay::setCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true)
{
    diskCounters.resetCounter(counterId);
    diskCounters.pegCounter(counterId, value);
}

void DiskColumnOverlay::setCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true)
{
    diskCounters.resetCounter(counterId);
    diskCounters.pegCounter(counterId, value);
}

void DiskColumnOverlay::resetState() noexcept(true)
{
    // for sure we have column overlay
    dynamic_cast<MemoryControllerColumnOverlay*>(memoryController.get())->resetState();
    diskCounters = MemoryCounters();
}

void DiskColumnOverlay::addStats(const Disk& disk) noexcept(true)
{
    MemoryControllerColumnOverlay* memoryController = dynamic_cast<MemoryControllerColumnOverlay*>(this->memoryController.get());
    MemoryModelColumnOverlay& memoryModel = const_cast<MemoryModelColumnOverlay&>(dynamic_cast<const MemoryModelColumnOverlay&>(memoryController->getMemoryModel()));

    const MemoryController& diskController = disk.getLowLevelController();
    const MemoryModel& diskMemoryModel = diskController.getMemoryModel();

    memoryModel.addWearOut(diskMemoryModel.getMemoryWearOut());

    for (enum MemoryCounters::MemoryCountersD counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++counterId)
        memoryController->pegCounter(counterId, diskController.getCounter(counterId).second);

    for (enum MemoryCounters::MemoryCountersL counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++counterId)
        memoryController->pegCounter(counterId, diskController.getCounter(counterId).second);

    for (enum MemoryCounters::MemoryCountersD counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_TIME; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME; ++counterId)
        pegCounter(counterId, disk.getDiskCounter(counterId).second);

    for (enum MemoryCounters::MemoryCountersL counterId = MemoryCounters::MEMORY_COUNTER_RW_READ_TOTAL_BYTES; counterId <= MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS; ++counterId)
        pegCounter(counterId, disk.getDiskCounter(counterId).second);
}

std::string DiskColumnOverlay::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskColumnOverlay {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskColumnOverlay {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}

std::string DiskColumnOverlay::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskColumnOverlay {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskColumnOverlay {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}