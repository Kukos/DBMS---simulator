#ifndef MEMORY_MODEL_COLUMN_OVERLAY
#define MEMORY_MODEL_COLUMN_OVERLAY

#include <storage/memoryModel.hpp>

class MemoryModelColumnOverlay : public MemoryModel
{
public:
    MemoryModelColumnOverlay(MemoryModel* model);
    MemoryModelColumnOverlay(const char* modelName, size_t pageSize, size_t blockSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new MemoryModel
    *
    * @return new MemoryModel
    */
    virtual MemoryModel* clone() const noexcept(true)
    {
        return new MemoryModelColumnOverlay(*this);
    }

    /**
     * @brief Write contiguous bytes to MemoryModel
     *
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation
     */
    double writeBytes(size_t bytes) noexcept(true) override;

    /**
     * @brief Write contiguous bytes on top of existing bytes to MemoryModel
     *
     * @param[in] bytes - bytes to overwrite
     *
     * @return time required for operation
     */
    double overwriteBytes(size_t bytes) noexcept(true) override;

    /**
     * @brief Write contiguous bytes to MemoryModel
     *
     * @param[in] bytes - bytes to write
     *
     * @return time required for operation
     */
    double readBytes(size_t bytes) noexcept(true) override;


    /**
     * @brief Created brief snapshot of MemoryModel as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of MemoryModel
     */
    std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of MemoryModel as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of MemoryModel
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true);

    /**
     * @brief Set the WearOut(touched bytes)
     *
     * @param[in] bytes - set touched bytes to bytes
     */
    void setWearOut(size_t bytes) noexcept(true);

    /**
     * @brief reset WearOut(touched bytes)
     *
     */
    void resetWearOut() noexcept(true);

    /**
     * @brief Add bytes to WearOut(touched bytes)
     *
     * @param[in] bytes - bytes to add
     */
    void addWearOut(size_t bytes) noexcept(true);

    /**
     * @brief Reset non-const values to default value
     *
     */
    void resetState() noexcept(true);

    virtual ~MemoryModelColumnOverlay() = default;
    MemoryModelColumnOverlay() = default;
    MemoryModelColumnOverlay(const MemoryModelColumnOverlay&) = default;
    MemoryModelColumnOverlay& operator=(const MemoryModelColumnOverlay&) = default;
    MemoryModelColumnOverlay(MemoryModelColumnOverlay &&) = default;
    MemoryModelColumnOverlay& operator=(MemoryModelColumnOverlay &&) = default;
};

#endif