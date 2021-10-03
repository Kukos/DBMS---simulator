#ifndef DISK_FLASH_NAND_FTL_HPP
#define DISK_FLASH_NAND_FTL_HPP

#include <disk/disk.hpp>
#include <storage/memoryControllerFlashNandFTL.hpp>

class DiskFlashNandFTL : public Disk
{
public:
    DiskFlashNandFTL(MemoryControllerFlashNandFTL* controller);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new Disk
    *
    * @return new Disk
    */
    virtual Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandFTL(*this);
    }

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

    DiskFlashNandFTL() = default;
    virtual ~DiskFlashNandFTL() = default;
    DiskFlashNandFTL(const DiskFlashNandFTL&) = default;
    DiskFlashNandFTL& operator=(const DiskFlashNandFTL&) = default;
    DiskFlashNandFTL(DiskFlashNandFTL &&) = default;
    DiskFlashNandFTL& operator=(DiskFlashNandFTL &&) = default;
};

class DiskFlashNandFTL_SamsungK9F1G08U0D : public DiskFlashNandFTL
{
public:
    DiskFlashNandFTL_SamsungK9F1G08U0D()
    : DiskFlashNandFTL(new MemoryControllerFlashNandFTL(new MemoryModelFlashNandFTL_SamsungK9F1G08U0D()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandFTL_SamsungK9F1G08U0D(*this);
    }


    ~DiskFlashNandFTL_SamsungK9F1G08U0D() = default;
    DiskFlashNandFTL_SamsungK9F1G08U0D(const DiskFlashNandFTL_SamsungK9F1G08U0D&) = default;
    DiskFlashNandFTL_SamsungK9F1G08U0D& operator=(const DiskFlashNandFTL_SamsungK9F1G08U0D&) = default;
    DiskFlashNandFTL_SamsungK9F1G08U0D(DiskFlashNandFTL_SamsungK9F1G08U0D &&) = default;
    DiskFlashNandFTL_SamsungK9F1G08U0D& operator=(DiskFlashNandFTL_SamsungK9F1G08U0D &&) = default;
};

class DiskFlashNandFTL_MicronMT29F32G08ABAAA : public DiskFlashNandFTL
{
public:
    DiskFlashNandFTL_MicronMT29F32G08ABAAA()
    : DiskFlashNandFTL(new MemoryControllerFlashNandFTL(new MemoryModelFlashNandFTL_MicronMT29F32G08ABAAA()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandFTL_MicronMT29F32G08ABAAA(*this);
    }

    ~DiskFlashNandFTL_MicronMT29F32G08ABAAA() = default;
    DiskFlashNandFTL_MicronMT29F32G08ABAAA(const DiskFlashNandFTL_MicronMT29F32G08ABAAA&) = default;
    DiskFlashNandFTL_MicronMT29F32G08ABAAA& operator=(const DiskFlashNandFTL_MicronMT29F32G08ABAAA&) = default;
    DiskFlashNandFTL_MicronMT29F32G08ABAAA(DiskFlashNandFTL_MicronMT29F32G08ABAAA &&) = default;
    DiskFlashNandFTL_MicronMT29F32G08ABAAA& operator=(DiskFlashNandFTL_MicronMT29F32G08ABAAA &&) = default;
};

class DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 : public DiskFlashNandFTL
{
public:
    DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1()
    : DiskFlashNandFTL(new MemoryControllerFlashNandFTL(new MemoryModelFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(*this);
    }

    ~DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1() = default;
    DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(const DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1& operator=(const DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1(DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
    DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1& operator=(DiskFlashNandFTL_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
};


#endif