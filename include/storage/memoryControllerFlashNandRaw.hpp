#ifndef MEMORY_CONTROLLER_FLASH_NAND_RAW_HPP
#define MEMORY_CONTROLLER_FLASH_NAND_RAW_HPP

#include <storage/memoryController.hpp>
#include <storage/memoryModelFlashNandRaw.hpp>

class MemoryControllerFlashNandRaw : public MemoryController
{
public:
    MemoryControllerFlashNandRaw(MemoryModelFlashNandRaw* flash);

    /**
     * @brief Created brief snapshot of Memory Controller as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Memory Controller
     */
    std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of Memory Controller as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Memory Controller
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true) override;

    ~MemoryControllerFlashNandRaw() = default;
    MemoryControllerFlashNandRaw() = default;
    MemoryControllerFlashNandRaw(const MemoryControllerFlashNandRaw&) = default;
    MemoryControllerFlashNandRaw& operator=(const MemoryControllerFlashNandRaw&) = default;
    MemoryControllerFlashNandRaw(MemoryControllerFlashNandRaw &&) = default;
    MemoryControllerFlashNandRaw& operator=(MemoryControllerFlashNandRaw &&) = default;
};

#endif