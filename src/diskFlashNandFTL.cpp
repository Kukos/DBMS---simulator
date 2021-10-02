#include <disk/diskFlashNandFTL.hpp>
#include <logger/logger.hpp>

DiskFlashNandFTL::DiskFlashNandFTL(MemoryControllerFlashNandFTL* controller)
: Disk(controller)
{
    LOGGER_LOG_DEBUG("Disk FlashNandFTL created: {}", toStringFull());
}

std::string DiskFlashNandFTL::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskFlashNandFTL {") +
                           std::string(" .memoryController = ") + memoryController->toString() +
                           std::string(" .diskCounters = ") + diskCounters.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskFlashNandFTL {\n") +
                           std::string("\t.memoryController = ") + memoryController->toString()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toString() + std::string("\n") +
                           std::string("}"));
}


std::string DiskFlashNandFTL::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("DiskFlashNandFTL {") +
                           std::string(" .memoryController = ") + memoryController->toStringFull() +
                           std::string(" .diskCounters = ") + diskCounters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("DiskFlashNandFTL {\n") +
                           std::string("\t.memoryController = ") + memoryController->toStringFull()  + std::string("\n") +
                           std::string("\t.diskCounters = ") + diskCounters.toStringFull() + std::string("\n") +
                           std::string("}"));
}