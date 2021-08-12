#ifndef MEMORY_MODEL_HPP
#define MEMORY_MODEL_HPP

#include <cstddef>
#include <string>

class MemoryModel
{
protected:
    const char* modelName;
    size_t pageSize; // page is a minimum number of bytes for writing and reading
    size_t blockSize; // block is a set of pages in case of block devices or just 0 in case of byte addresed devices
    size_t touchedBytes; // how many bytes we programmed? This is called memory wear-out

    size_t bytesToPages(size_t bytes) const noexcept(true);
    size_t bytesToBlocks(size_t bytes) const noexcept(true);

public:
    /**
     * @brief Write contiguous bytes to MemoryModel
     *
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation
     */
    virtual double writeBytes(size_t bytes) noexcept(true) = 0;

    /**
     * @brief Write contiguous bytes on top of existing bytes to MemoryModel
     *
     * @param[in] bytes - bytes to overwrite
     *
     * @return time required for operation
     */
    virtual double overwriteBytes(size_t bytes) noexcept(true) = 0;

    /**
     * @brief Write contiguous bytes to MemoryModel
     *
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation
     */
    virtual double readBytes(size_t bytes) noexcept(true) = 0;

    /**
     * @brief Get model name
     *
     * @return model name as const char*
     */
    virtual const char* getModelName() const noexcept(true)
    {
        return modelName;
    }

    /**
     * @brief Get page size in Bytes
     * Page is a minimum unit (number of bytes) for wrtie and read operations
     *
     * @return page size
     */
    virtual size_t getPageSize() const noexcept(true)
    {
        return pageSize;
    }

    /**
     * @brief Get block size in Bytes
     * Block is a set of pages in case of block devices.
     * Otherwise is equal to 0
     *
     * @return block size
     */
    virtual size_t getBlockSize() const noexcept(true)
    {
        return blockSize;
    }

    /**
     * @brief Get Memory Wear-out
     * Wear-out is the number of bytes touched (programmed)
     *
     * @return wear-out in bytes
     */
    virtual size_t getMemoryWearOut() const noexcept(true)
    {
        return touchedBytes;
    }

    /**
     * @brief Created brief snapshot of MemoryModel as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of MemoryModel
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of MemoryModel as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of MemoryModel
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    virtual ~MemoryModel() = default;

    MemoryModel() = default;
    MemoryModel(const MemoryModel&) = default;
    MemoryModel& operator=(const MemoryModel&) = default;
    MemoryModel(MemoryModel &&) = default;
    MemoryModel& operator=(MemoryModel &&) = default;

    MemoryModel(const char* modelName, size_t pageSize, size_t blockSize);
};

#endif
