#ifndef MEMORY_MODEL_SSD_HPP
#define MEMORY_MODEL_SSD_HPP

#include <storage/memoryModel.hpp>

class MemoryModelSSD : public MemoryModel
{
private:
    static constexpr size_t seqOpTreshold = 4;

    size_t pagesInBlock; // how many pages are in 1 block

    double readRandomTime; // in s per page
    double writeRandomTime; // in s per page
    double readSeqTime; // in s per page
    double writeSeqTime; // in s per page
    double eraseTime; // in s per block

    size_t dirtyPages; // how many pages are dirty (waiting to erase)

    double readPagesRandom(size_t pages) const noexcept(true);
    double writePagesRandom(size_t pages) noexcept(true);
    double readPagesSeq(size_t pages) const noexcept(true);
    double writePagesSeq(size_t pages) noexcept(true);
    double eraseBlocks(size_t blocks) noexcept(true);

    double flushDirtyPages() noexcept(true);

public:
    virtual ~MemoryModelSSD() = default;
    MemoryModelSSD() = default;
    MemoryModelSSD(const MemoryModelSSD&) = default;
    MemoryModelSSD& operator=(const MemoryModelSSD&) = default;
    MemoryModelSSD(MemoryModelSSD &&) = default;
    MemoryModelSSD& operator=(MemoryModelSSD &&) = default;

    MemoryModelSSD(const char* modelName,
                   size_t pageSize,
                   size_t blockSize,
                   double readRandomTime,
                   double writeRandomTime,
                   double readSeqTime,
                   double writeSeqTime,
                   double eraseTime);

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
    std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of MemoryModel as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of MemoryModel
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true) override;
};


class MemoryModelSSD_Samsung840 : public MemoryModelSSD
{
public:
    MemoryModelSSD_Samsung840()
    : MemoryModelSSD("SSD:samsung840", 8192, 8192 * 64, 21.0 / 1000000.0, 45.0 / 1000000.0, 14.0 / 1000000.0, 15.3 / 1000000.0, 210.0 * 64.0 / 1000000.0)
    {

    }
    ~MemoryModelSSD_Samsung840() = default;
    MemoryModelSSD_Samsung840(const MemoryModelSSD_Samsung840&) = default;
    MemoryModelSSD_Samsung840& operator=(const MemoryModelSSD_Samsung840&) = default;
    MemoryModelSSD_Samsung840(MemoryModelSSD_Samsung840 &&) = default;
    MemoryModelSSD_Samsung840& operator=(MemoryModelSSD_Samsung840 &&) = default;
};

class MemoryModelSSD_IntelDCP4511 : public MemoryModelSSD
{
public:
    MemoryModelSSD_IntelDCP4511()
    : MemoryModelSSD("SSD:intelDCP4511", 4096, 4096 * 64, 3.3 / 1000000.0, 27.7 / 1000000.0, 2.0 / 1000000.0, 2.75 / 1000000.0, 277.0 * 64.0 / 1000000.0)
    {

    }

    ~MemoryModelSSD_IntelDCP4511() = default;
    MemoryModelSSD_IntelDCP4511(const MemoryModelSSD_IntelDCP4511&) = default;
    MemoryModelSSD_IntelDCP4511& operator=(const MemoryModelSSD_IntelDCP4511&) = default;
    MemoryModelSSD_IntelDCP4511(MemoryModelSSD_IntelDCP4511 &&) = default;
    MemoryModelSSD_IntelDCP4511& operator=(MemoryModelSSD_IntelDCP4511 &&) = default;
};

class MemoryModelSSD_ToshibaVX500 : public MemoryModelSSD
{
public:
    MemoryModelSSD_ToshibaVX500()
    : MemoryModelSSD("SSD:toshibaVX500", 4096, 4096 * 64, 10.8 / 1000000.0, 15.3 / 1000000.0, 7.2 / 1000000.0, 7.8 / 1000000.0, 153.0 * 64.0 / 1000000.0)
    {

    }

    ~MemoryModelSSD_ToshibaVX500() = default;
    MemoryModelSSD_ToshibaVX500(const MemoryModelSSD_ToshibaVX500&) = default;
    MemoryModelSSD_ToshibaVX500& operator=(const MemoryModelSSD_ToshibaVX500&) = default;
    MemoryModelSSD_ToshibaVX500(MemoryModelSSD_ToshibaVX500 &&) = default;
    MemoryModelSSD_ToshibaVX500& operator=(MemoryModelSSD_ToshibaVX500 &&) = default;
};

#endif
