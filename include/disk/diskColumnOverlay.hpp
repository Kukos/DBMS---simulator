#ifndef DISK_COLUMN_OVERLAY_HPP
#define DISK_COLUMN_OVERLAY_HPP

#include <disk/disk.hpp>

class DiskColumnOverlay : public Disk
{
public:
    DiskColumnOverlay(MemoryController* controller);
    DiskColumnOverlay(Disk* disk);

    virtual Disk* clone() const noexcept(true) override
    {
        return new DiskColumnOverlay(*this);
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
     * @brief Add (Aggregate) stats from disk into overlay
     *
     * @param[in] disk - disk from stats will be aggregated
     */
    void addStats(const Disk& disk) noexcept(true);

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

    virtual ~DiskColumnOverlay() = default;
    DiskColumnOverlay() = default;
    DiskColumnOverlay(const DiskColumnOverlay&) = default;
    DiskColumnOverlay& operator=(const DiskColumnOverlay&) = default;
    DiskColumnOverlay(DiskColumnOverlay &&) = default;
    DiskColumnOverlay& operator=(DiskColumnOverlay &&) = default;
};

#endif