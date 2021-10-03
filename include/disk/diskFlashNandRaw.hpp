#ifndef DISK_FLASH_NAND_RAW_HPP
#define DISK_FLASH_NAND_RAW_HPP

#include <disk/disk.hpp>
#include <storage/memoryControllerFlashNandRaw.hpp>

class DiskFlashNandRaw : public Disk
{
public:
    DiskFlashNandRaw(MemoryControllerFlashNandRaw* controller);

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
    * @brief Virtual constructor idiom implemented as clone function. This function creates new Disk
    *
    * @return new Disk
    */
    virtual Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandRaw(*this);
    }


    DiskFlashNandRaw() = default;
    virtual ~DiskFlashNandRaw() = default;
    DiskFlashNandRaw(const DiskFlashNandRaw&) = default;
    DiskFlashNandRaw& operator=(const DiskFlashNandRaw&) = default;
    DiskFlashNandRaw(DiskFlashNandRaw &&) = default;
    DiskFlashNandRaw& operator=(DiskFlashNandRaw &&) = default;
};

class DiskFlashNandRaw_SamsungK9F1G08U0D : public DiskFlashNandRaw
{
public:
    DiskFlashNandRaw_SamsungK9F1G08U0D()
    : DiskFlashNandRaw(new MemoryControllerFlashNandRaw(new MemoryModelFlashNandRaw_SamsungK9F1G08U0D()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandRaw_SamsungK9F1G08U0D(*this);
    }

    ~DiskFlashNandRaw_SamsungK9F1G08U0D() = default;
    DiskFlashNandRaw_SamsungK9F1G08U0D(const DiskFlashNandRaw_SamsungK9F1G08U0D&) = default;
    DiskFlashNandRaw_SamsungK9F1G08U0D& operator=(const DiskFlashNandRaw_SamsungK9F1G08U0D&) = default;
    DiskFlashNandRaw_SamsungK9F1G08U0D(DiskFlashNandRaw_SamsungK9F1G08U0D &&) = default;
    DiskFlashNandRaw_SamsungK9F1G08U0D& operator=(DiskFlashNandRaw_SamsungK9F1G08U0D &&) = default;
};

class DiskFlashNandRaw_MicronMT29F32G08ABAAA : public DiskFlashNandRaw
{
public:
    DiskFlashNandRaw_MicronMT29F32G08ABAAA()
    : DiskFlashNandRaw(new MemoryControllerFlashNandRaw(new MemoryModelFlashNandRaw_MicronMT29F32G08ABAAA()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandRaw_MicronMT29F32G08ABAAA(*this);
    }

    ~DiskFlashNandRaw_MicronMT29F32G08ABAAA() = default;
    DiskFlashNandRaw_MicronMT29F32G08ABAAA(const DiskFlashNandRaw_MicronMT29F32G08ABAAA&) = default;
    DiskFlashNandRaw_MicronMT29F32G08ABAAA& operator=(const DiskFlashNandRaw_MicronMT29F32G08ABAAA&) = default;
    DiskFlashNandRaw_MicronMT29F32G08ABAAA(DiskFlashNandRaw_MicronMT29F32G08ABAAA &&) = default;
    DiskFlashNandRaw_MicronMT29F32G08ABAAA& operator=(DiskFlashNandRaw_MicronMT29F32G08ABAAA &&) = default;
};

class DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 : public DiskFlashNandRaw
{
public:
    DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1()
    : DiskFlashNandRaw(new MemoryControllerFlashNandRaw(new MemoryModelFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1(*this);
    }


    ~DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1() = default;
    DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1(const DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1& operator=(const DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1&) = default;
    DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1(DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
    DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1& operator=(DiskFlashNandRaw_MicronMT29F32G08CBEDBL83A3WC1 &&) = default;
};


#endif