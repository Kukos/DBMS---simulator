#ifndef MEMORY_MODEL_FLASH_NAND_RAW_HPP
#define MEMORY_MODEL_FLASH_NAND_RAW_HPP

#include <storage/memoryModel.hpp>

class MemoryModelFlashNandRaw : public MemoryModel
{
private:
    double readTime; // in s per page
    double writeTime; // in s per page
    double eraseTime; // in s per block

    double readPages(size_t pages) const noexcept(true);
    double writePages(size_t pages) noexcept(true);
    double eraseBlocks(size_t blocks) noexcept(true);

public:
    virtual ~MemoryModelFlashNandRaw() = default;
    MemoryModelFlashNandRaw() = default;
    MemoryModelFlashNandRaw(const MemoryModelFlashNandRaw&) = default;
    MemoryModelFlashNandRaw& operator=(const MemoryModelFlashNandRaw&) = default;
    MemoryModelFlashNandRaw(MemoryModelFlashNandRaw &&) = default;
    MemoryModelFlashNandRaw& operator=(MemoryModelFlashNandRaw &&) = default;

    MemoryModelFlashNandRaw(const char* modelName,
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
class MemoryModelFlashNandRaw_SamsungK9F1G08U0D : public MemoryModelFlashNandRaw
{
public:
    MemoryModelFlashNandRaw_SamsungK9F1G08U0D()
    : MemoryModelFlashNandRaw("FlashNandRaw:samsungK9F1G08U0D", 2048, 2048 * 32, 35.0 / 1000000.0, 250.0 / 1000000.0, 2000.0 / 1000000.0)
    {

    }

    ~MemoryModelFlashNandRaw_SamsungK9F1G08U0D() = default;
    MemoryModelFlashNandRaw_SamsungK9F1G08U0D(const MemoryModelFlashNandRaw_SamsungK9F1G08U0D&) = default;
    MemoryModelFlashNandRaw_SamsungK9F1G08U0D& operator=(const MemoryModelFlashNandRaw_SamsungK9F1G08U0D&) = default;
    MemoryModelFlashNandRaw_SamsungK9F1G08U0D(MemoryModelFlashNandRaw_SamsungK9F1G08U0D &&) = default;
    MemoryModelFlashNandRaw_SamsungK9F1G08U0D& operator=(MemoryModelFlashNandRaw_SamsungK9F1G08U0D &&) = default;
};

// https://pl.mouser.com/datasheet/2/671/micron_technology_micts05995-1-1759202.pdf
class MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA : public MemoryModelFlashNandRaw
{
public:
    MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA()
    : MemoryModelFlashNandRaw("FlashNandRaw:micronMT29F32G08ABAAA", 8192, 8192 * 128, 35.0 / 1000000.0, 350.0 / 1000000.0, 1500.0 / 1000000.0)
    {

    }

    ~MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA() = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA(const MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA& operator=(const MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA(MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA &&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA& operator=(MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA &&) = default;
};

// https://www.avnet.com/shop/us/products/micron/mt29f32g08cbedbl83a3wc1-3074457345628275270/
class MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 : public MemoryModelFlashNandRaw
{
public:
    MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1()
    : MemoryModelFlashNandRaw("FlashNandRaw:micronMT29F32G08CBEDBL83A3WC1", 4096, 4096 * 128, 50.0 / 1000000.0, 900.0 / 1000000.0, 3500 / 1000000.0)
    {

    }

    ~MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1() = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1(const MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1& operator=(const MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1(MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
    MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1& operator=(MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
};

#endif