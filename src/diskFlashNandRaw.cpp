#include <disk/diskFlashNandRaw.hpp>
#include <logger/logger.hpp>

DiskFlashNandRaw::DiskFlashNandRaw(MemoryControllerFlashNandRaw* controller)
: Disk(controller)
{
    LOGGER_LOG_DEBUG("Disk FlashNandRaw created: {}", toStringFull());
}

std::string DiskFlashNandRaw::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskFlashNandRaw {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskFlashNandRaw {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}


std::string DiskFlashNandRaw::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskFlashNandRaw {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskFlashNandRaw {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}