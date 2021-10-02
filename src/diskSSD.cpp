#include <disk/diskSSD.hpp>
#include <logger/logger.hpp>

DiskSSD::DiskSSD(MemoryControllerSSD* controller)
: Disk(controller)
{
    LOGGER_LOG_DEBUG("Disk SSD created: {}", toStringFull());
}

std::string DiskSSD::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskSSD {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskSSD {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}


std::string DiskSSD::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskSDD {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskSSD {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}