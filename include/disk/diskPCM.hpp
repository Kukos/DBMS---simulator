#ifndef DISK_PCM_HPP
#define DISK_PCM_HPP

#include <disk/disk.hpp>
#include <storage/memoryControllerPCM.hpp>

class DiskPCM : public Disk
{
public:
    DiskPCM(MemoryControllerPCM* controller);

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

    DiskPCM() = default;
    virtual ~DiskPCM() = default;
    DiskPCM(const DiskPCM&) = default;
    DiskPCM& operator=(const DiskPCM&) = default;
    DiskPCM(DiskPCM &&) = default;
    DiskPCM& operator=(DiskPCM &&) = default;
};

class DiskPCM_DefaultModel : public DiskPCM
{
public:
    DiskPCM_DefaultModel()
    : DiskPCM(new MemoryControllerPCM(new MemoryModelPCM_DefaultModel()))
    {

    }

    ~DiskPCM_DefaultModel() = default;
    DiskPCM_DefaultModel(const DiskPCM_DefaultModel&) = default;
    DiskPCM_DefaultModel& operator=(const DiskPCM_DefaultModel&) = default;
    DiskPCM_DefaultModel(DiskPCM_DefaultModel &&) = default;
    DiskPCM_DefaultModel& operator=(DiskPCM_DefaultModel &&) = default;
};

#endif