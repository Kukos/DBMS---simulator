#ifndef DISK_SSD_HPP
#define DISK_SSD_HPP

#include <disk/disk.hpp>
#include <storage/memoryControllerSSD.hpp>

class DiskSSD : public Disk
{
public:
    DiskSSD(MemoryControllerSSD* controller);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new Disk
    *
    * @return new Disk
    */
    virtual Disk* clone() const noexcept(true) override
    {
        return new DiskSSD(*this);
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

    DiskSSD() = default;
    virtual ~DiskSSD() = default;
    DiskSSD(const DiskSSD&) = default;
    DiskSSD& operator=(const DiskSSD&) = default;
    DiskSSD(DiskSSD &&) = default;
    DiskSSD& operator=(DiskSSD &&) = default;
};

class DiskSSD_Samsung840 : public DiskSSD
{
public:
    DiskSSD_Samsung840()
    : DiskSSD(new MemoryControllerSSD(new MemoryModelSSD_Samsung840()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskSSD_Samsung840(*this);
    }

    ~DiskSSD_Samsung840() = default;
    DiskSSD_Samsung840(const DiskSSD_Samsung840&) = default;
    DiskSSD_Samsung840& operator=(const DiskSSD_Samsung840&) = default;
    DiskSSD_Samsung840(DiskSSD_Samsung840 &&) = default;
    DiskSSD_Samsung840& operator=(DiskSSD_Samsung840 &&) = default;
};

class DiskSSD_IntelDCP4511 : public DiskSSD
{
public:
    DiskSSD_IntelDCP4511()
    : DiskSSD(new MemoryControllerSSD(new MemoryModelSSD_IntelDCP4511()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskSSD_IntelDCP4511(*this);
    }

    ~DiskSSD_IntelDCP4511() = default;
    DiskSSD_IntelDCP4511(const DiskSSD_IntelDCP4511&) = default;
    DiskSSD_IntelDCP4511& operator=(const DiskSSD_IntelDCP4511&) = default;
    DiskSSD_IntelDCP4511(DiskSSD_IntelDCP4511 &&) = default;
    DiskSSD_IntelDCP4511& operator=(DiskSSD_IntelDCP4511 &&) = default;
};

class DiskSSD_ToshibaVX500 : public DiskSSD
{
public:
    DiskSSD_ToshibaVX500()
    : DiskSSD(new MemoryControllerSSD(new MemoryModelSSD_ToshibaVX500()))
    {

    }

    Disk* clone() const noexcept(true) override
    {
        return new DiskSSD_ToshibaVX500(*this);
    }

    ~DiskSSD_ToshibaVX500() = default;
    DiskSSD_ToshibaVX500(const DiskSSD_ToshibaVX500&) = default;
    DiskSSD_ToshibaVX500& operator=(const DiskSSD_ToshibaVX500&) = default;
    DiskSSD_ToshibaVX500(DiskSSD_ToshibaVX500 &&) = default;
    DiskSSD_ToshibaVX500& operator=(DiskSSD_ToshibaVX500 &&) = default;
};

#endif