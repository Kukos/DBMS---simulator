#ifndef MEMORY_CONTROLLER_COLUMN_OVERLAY_HPP
#define MEMORY_CONTROLLER_COLUMN_OVERLAY_HPP

#include <storage/memoryController.hpp>

class MemoryControllerColumnOverlay : public MemoryController
{
public:
    MemoryControllerColumnOverlay(MemoryModel* model);
    MemoryControllerColumnOverlay(MemoryController* controller);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new MemoryController
    *
    * @return new MemoryController
    */
    virtual MemoryController* clone() const noexcept(true) override
    {
        return new MemoryControllerColumnOverlay(*this);
    }

        /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true);

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true);

    /**
     * @brief SET counter to value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be set to this value
     */
    void setCounter(enum MemoryCounters::MemoryCountersL counterId, long value) noexcept(true);

    /**
     * @brief SET counter to value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be set to this value
     */
    void setCounter(enum MemoryCounters::MemoryCountersD counterId, double value) noexcept(true);

    /**
     * @brief Created brief snapshot of Disk as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Disk
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of Disk as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Disk
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Set the Memory Model object
     *
     * @param[in] model
     */
    void setMemoryModel(const MemoryModel& model) noexcept(true);

    /**
     * @brief Get the Memory Model object
     *
     * @return MemoryModel&
     */
    MemoryModel& getMemoryModel() noexcept(true);

    virtual ~MemoryControllerColumnOverlay() = default;
    MemoryControllerColumnOverlay() = default;
    MemoryControllerColumnOverlay(const MemoryControllerColumnOverlay&) = default;
    MemoryControllerColumnOverlay& operator=(const MemoryControllerColumnOverlay&) = default;
    MemoryControllerColumnOverlay(MemoryControllerColumnOverlay &&) = default;
    MemoryControllerColumnOverlay& operator=(MemoryControllerColumnOverlay &&) = default;
};

#endif