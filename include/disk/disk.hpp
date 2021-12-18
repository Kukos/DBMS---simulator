#ifndef DISK_HPP
#define DISK_HPP

#include <storage/memoryController.hpp>
#include <observability/memoryCounters.hpp>
#include <memory>

class Disk
{
protected:
    MemoryCounters diskCounters;
    std::unique_ptr<MemoryController> memoryController;

public:
    /**
     * @brief Get Low Level MemoryController in read only access mode
     *        You can use this function to read controller counters, name and other features
     *
     * @return const reference to memory controller
     */
    const MemoryController& getLowLevelController() const noexcept(true);

    /**
     * @brief Get current logical memory adress.
     * Logical address is used to cache pages / blocks
     *
     * @return current logical address
     */
    uintptr_t getCurrentMemoryAddr() const noexcept(true);

    /**
     * @brief Flush cache, write down all pages in QUEUE, clear cache
     *
     * @return time needed to write down all pages from QUEUE
     */
    double flushCache() noexcept(true);

    /**
     * @brief Read contiguous bytes from memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to read
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    double readBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Write contiguous bytes to memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    double writeBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Overwrite contiguous bytes to memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to overwrite
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    double overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getDiskCounter(enum MemoryCounters::MemoryCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getDiskCounter(enum MemoryCounters::MemoryCountersL counterId) const noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetDiskCounter(enum MemoryCounters::MemoryCountersL counterId) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetDiskCounter(enum MemoryCounters::MemoryCountersD counterId) noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllDiskCounters() noexcept(true);

    /**
     * @brief Created brief snapshot of Disk as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Disk
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of Disk as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Disk
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    Disk(MemoryController* controller);

    virtual ~Disk() = default;
    Disk() = default;

    Disk(const Disk&);
    Disk& operator=(const Disk&);

    Disk(Disk &&) = default;
    Disk& operator=(Disk &&) = default;
};

#endif
