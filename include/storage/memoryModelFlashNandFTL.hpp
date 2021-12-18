#ifndef MEMORY_MODEL_FLASH_NAND_FTL_HPP
#define MEMORY_MODEL_FLASH_NAND_FTL_HPP

#include <storage/memoryModel.hpp>

class MemoryModelFlashNandFTL : public MemoryModel
{
private:
    size_t pagesInBlock; // how many pages are in 1 block

    double readTime; // in s per page
    double writeTime; // in s per page
    double eraseTime; // in s per block

    size_t dirtyPages; // how many pages are dirty (waiting to erase)

    double readPages(size_t pages) const noexcept(true);
    double writePages(size_t pages) noexcept(true);
    double eraseBlocks(size_t blocks) noexcept(true);

    double flushDirtyPages() noexcept(true);

public:
    virtual MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelFlashNandFTL(*this);
    }

    virtual ~MemoryModelFlashNandFTL() = default;
    MemoryModelFlashNandFTL() = default;
    MemoryModelFlashNandFTL(const MemoryModelFlashNandFTL&) = default;
    MemoryModelFlashNandFTL& operator=(const MemoryModelFlashNandFTL&) = default;
    MemoryModelFlashNandFTL(MemoryModelFlashNandFTL &&) = default;
    MemoryModelFlashNandFTL& operator=(MemoryModelFlashNandFTL &&) = default;

    MemoryModelFlashNandFTL(const char* modelName,
                            size_t pageSize,
                            size_t blockSize,
                            double readTime,
                            double writeTime,
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

// http://www.tech-blog.pl/wordpress/wp-content/uploads/2013/10/k9f1g08u0d_00.pdf
class MemoryModelFlashNandFTL_SamsungK9F1G08U0D : public MemoryModelFlashNandFTL
{
public:
    MemoryModelFlashNandFTL_SamsungK9F1G08U0D()
    : MemoryModelFlashNandFTL("FlashNandFTL:samsungK9F1G08U0D", 2048, 2048 * 32, 35.0 / 1000000.0, 250.0 / 1000000.0, 2000.0 / 1000000.0)
    {

    }

    MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelFlashNandFTL_SamsungK9F1G08U0D(*this);
    }

    ~MemoryModelFlashNandFTL_SamsungK9F1G08U0D() = default;
    MemoryModelFlashNandFTL_SamsungK9F1G08U0D(const MemoryModelFlashNandFTL_SamsungK9F1G08U0D&) = default;
    MemoryModelFlashNandFTL_SamsungK9F1G08U0D& operator=(const MemoryModelFlashNandFTL_SamsungK9F1G08U0D&) = default;
    MemoryModelFlashNandFTL_SamsungK9F1G08U0D(MemoryModelFlashNandFTL_SamsungK9F1G08U0D &&) = default;
    MemoryModelFlashNandFTL_SamsungK9F1G08U0D& operator=(MemoryModelFlashNandFTL_SamsungK9F1G08U0D &&) = default;
};

// https://pl.mouser.com/datasheet/2/671/micron_technology_micts05995-1-1759202.pdf
class MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA : public MemoryModelFlashNandFTL
{
public:
    MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA()
    : MemoryModelFlashNandFTL("FlashNandFTL:micronMT29F32G08ABAAA", 8192, 8192 * 128, 35.0 / 1000000.0, 350.0 / 1000000.0, 1500.0 / 1000000.0)
    {

    }

    MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA(*this);
    }


    ~MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA() = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA(const MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA& operator=(const MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA(MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA &&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA& operator=(MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA &&) = default;
};

// https://www.avnet.com/shop/us/products/micron/mt29f32g08cbedbl83a3wc1-3074457345628275270/
class MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 : public MemoryModelFlashNandFTL
{
public:
    MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1()
    : MemoryModelFlashNandFTL("FlashNandFTL:micronMT29F32G08CBEDBL83A3WC1", 4096, 4096 * 128, 50.0 / 1000000.0, 900.0 / 1000000.0, 3500 / 1000000.0)
    {

    }

    MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(*this);
    }

    ~MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1() = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(const MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1& operator=(const MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
    MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1& operator=(MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
};

#endif