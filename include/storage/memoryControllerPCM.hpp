#ifndef MEMORY_CONTROLLER_PCM_HPP
#define MEMORY_CONTROLLER_PCM_HPP

#include <storage/memoryController.hpp>
#include <storage/memoryModelPCM.hpp>

class MemoryControllerPCM : public MemoryController
{
public:
    MemoryControllerPCM(MemoryModelPCM* pcm);

    MemoryController* clone() const noexcept(true) override
    {
        return new MemoryControllerPCM(*this);
    }

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

    ~MemoryControllerPCM() = default;
    MemoryControllerPCM() = default;
    MemoryControllerPCM(const MemoryControllerPCM&) = default;
    MemoryControllerPCM& operator=(const MemoryControllerPCM&) = default;
    MemoryControllerPCM(MemoryControllerPCM &&) = default;
    MemoryControllerPCM& operator=(MemoryControllerPCM &&) = default;
};

#endif