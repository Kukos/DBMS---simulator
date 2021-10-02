#include <disk/diskPCM.hpp>
#include <logger/logger.hpp>

DiskPCM::DiskPCM(MemoryControllerPCM* controller)
: Disk(controller)
{
    LOGGER_LOG_DEBUG("Disk PCM created: {}", toStringFull());
}

std::string DiskPCM::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskPCM {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskPCM {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}


std::string DiskPCM::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskPCM {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskPCM {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}