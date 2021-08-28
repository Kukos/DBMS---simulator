#ifndef MEMORY_CONTROLLER_FLASH_NAND_FTL_HPP
#define MEMORY_CONTROLLER_FLASH_NAND_FTL_HPP

#include <storage/memoryController.hpp>
#include <storage/memoryModelFlashNandFTL.hpp>

class MemoryControllerFlashNandFTL : public MemoryController
{
public:
    MemoryControllerFlashNandFTL(MemoryModelFlashNandFTL* flash);

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

    ~MemoryControllerFlashNandFTL() = default;
    MemoryControllerFlashNandFTL() = default;
    MemoryControllerFlashNandFTL(const MemoryControllerFlashNandFTL&) = default;
    MemoryControllerFlashNandFTL& operator=(const MemoryControllerFlashNandFTL&) = default;
    MemoryControllerFlashNandFTL(MemoryControllerFlashNandFTL &&) = default;
    MemoryControllerFlashNandFTL& operator=(MemoryControllerFlashNandFTL &&) = default;
};

#endif