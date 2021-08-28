#ifndef MEMORY_CONTROLLER_SSD_HPP
#define MEMORY_CONTROLLER_SSD_HPP

#include <storage/memoryController.hpp>
#include <storage/memoryModelSSD.hpp>

class MemoryControllerSSD : public MemoryController
{
public:
    MemoryControllerSSD(MemoryModelSSD* ssd);

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

    ~MemoryControllerSSD() = default;
    MemoryControllerSSD() = default;
    MemoryControllerSSD(const MemoryControllerSSD&) = default;
    MemoryControllerSSD& operator=(const MemoryControllerSSD&) = default;
    MemoryControllerSSD(MemoryControllerSSD &&) = default;
    MemoryControllerSSD& operator=(MemoryControllerSSD &&) = default;
};

#endif