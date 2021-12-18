#ifndef MEMORY_MODEL_PCM_HPP
#define MEMORY_MODEL_PCM_HPP

#include <storage/memoryModel.hpp>

class MemoryModelPCM : public MemoryModel
{
private:
    double readTime;
    double writeTime;

    double readMemLines(size_t memLines) const noexcept(true);
    double writeMemLines(size_t memLines) noexcept(true);

public:
    virtual MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelPCM(*this);
    }

    virtual ~MemoryModelPCM() = default;
    MemoryModelPCM() = default;
    MemoryModelPCM(const MemoryModelPCM&) = default;
    MemoryModelPCM& operator=(const MemoryModelPCM&) = default;
    MemoryModelPCM(MemoryModelPCM &&) = default;
    MemoryModelPCM& operator=(MemoryModelPCM &&) = default;

    MemoryModelPCM(const char* modelName,
                   size_t memLine,
                   double readTime,
                   double writeTime);

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


class MemoryModelPCM_DefaultModel : public MemoryModelPCM
{
public:
    MemoryModelPCM_DefaultModel()
    : MemoryModelPCM("PCM:defaultModel", 64, 50.0 / 1000000000.0, 1.0 / 1000000)
    {

    }

    MemoryModel* clone() const noexcept(true) override
    {
        return new MemoryModelPCM_DefaultModel(*this);
    }

    ~MemoryModelPCM_DefaultModel() = default;
    MemoryModelPCM_DefaultModel(const MemoryModelPCM_DefaultModel&) = default;
    MemoryModelPCM_DefaultModel& operator=(const MemoryModelPCM_DefaultModel&) = default;
    MemoryModelPCM_DefaultModel(MemoryModelPCM_DefaultModel &&) = default;
    MemoryModelPCM_DefaultModel& operator=(MemoryModelPCM_DefaultModel &&) = default;
};

#endif