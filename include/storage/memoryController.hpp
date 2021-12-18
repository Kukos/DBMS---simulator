#ifndef MEMORY_CONTROLLER_HPP
#define MEMORY_CONTROLLER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include <storage/memoryModel.hpp>
#include <observability/memoryCounters.hpp>

class MemoryController
{
private:
    bool isAddrAlignedToCacheLine(uintptr_t addr, size_t cacheLineSize) const noexcept(true);

protected:
    MemoryCounters counters;
    std::unique_ptr<MemoryModel> memoryModel;
    size_t readCacheLineSize;
    size_t writeCacheLineSize;
    std::vector<size_t> readCache;
    std::vector<size_t> writeCache;
    std::vector<std::pair<size_t, std::vector<bool>>> overwriteCache;

    virtual size_t addrToCacheLineIndex(uintptr_t addr, size_t cacheLineSize) const noexcept(true);

public:
    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new MemoryController
    *
    * @return new MemoryController
    */
    virtual MemoryController* clone() const noexcept(true)
    {
        return new MemoryController(*this);
    }

    /**
     * @brief Get memory model name
     *
     * @return Memory Model Name as const char*
     */
    virtual const char* getModelName() const noexcept(true);

    /**
     * @brief Get the Memory Model object
     *
     * @return const MemoryModel&
     */
    virtual const MemoryModel& getMemoryModel() const noexcept(true)
    {
        return *memoryModel;
    }

    /**
     * @brief Get page size in Bytes
     * Page is a minimum unit (number of bytes) for wrtie and read operations
     *
     * @return page size
     */
    virtual size_t getPageSize() const noexcept(true);

    /**
     * @brief Get block size in Bytes
     * Block is a set of pages in case of block devices.
     * Otherwise is equal to 0
     *
     * @return block size
     */
    virtual size_t getBlockSize() const noexcept(true);


    /**
     * @brief Get Memory Wear-out
     * Wear-out is the number of bytes touched (programmed)
     *
     * @return wear-out in bytes
     */
    virtual size_t getMemoryWearOut() const noexcept(true);

    /**
     * @brief Get current logical memory adress.
     * Logical address is used to cache pages / blocks
     *
     * @return current logical address
     */
    virtual uintptr_t getCurrentMemoryAddr() const noexcept(true);

    /**
     * @brief Flush cache, write down all pages in QUEUE, clear cache
     *
     * @return time needed to write down all pages from QUEUE
     */
    virtual double flushCache() noexcept(true);

    /**
     * @brief Read contiguous bytes from memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to read
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    virtual double readBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Write contiguous bytes to memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    virtual double writeBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Overwrite contiguous bytes to memory from address @addr
     *
     * @param[in] addr - start address
     * @param[in] bytes - bytes to overwrite
     *
     * @return time required for operation, could be 0 if there is no cache miss
     */
    virtual double overwriteBytes(uintptr_t addr, size_t bytes) noexcept(true);

    /**
     * @brief Created brief snapshot of Memory Controller as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Memory Controller
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of Memory Controller as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Memory Controller
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getCounter(enum MemoryCounters::MemoryCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getCounter(enum MemoryCounters::MemoryCountersL counterId) const noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum MemoryCounters::MemoryCountersL counterId) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum MemoryCounters::MemoryCountersD counterId) noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllCounters() noexcept(true);

    MemoryController(MemoryModel* memoryModel);
    MemoryController(MemoryModel* memoryModel, size_t readCacheLineSize, size_t writeCacheLineSize);

    virtual ~MemoryController() = default;
    MemoryController() = default;

    MemoryController(const MemoryController&);
    MemoryController& operator=(const MemoryController&);

    MemoryController(MemoryController &&) = default;
    MemoryController& operator=(MemoryController &&) = default;
};

#endif