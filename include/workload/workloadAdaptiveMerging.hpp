#ifndef WORKLOAD_ADAPTIVE_MERGING_HPP
#define WORKLOAD_ADAPTIVE_MERGING_HPP

#include <workload/workload.hpp>

class WorkloadAdaptiveMerging : public Workload
{
protected:
    double sel;

public:
    /**
     * @brief Construct a new Workload Adaptive Merging object
     *
     * @param[in] indexes - vector of Indexes
     * @param[in] sel - step selectivty
     */
    WorkloadAdaptiveMerging(const std::vector<DBIndex*>& indexes, double sel);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new Workload
    *
    * @return new Workload
    */
    virtual Workload* clone() const noexcept(true) override
    {
        return new WorkloadAdaptiveMerging(*this);
    };

    /**
     * @brief Run all steps for all indexes
     *
     */
    virtual void run() noexcept(true) override;

    /**
     * @brief Created brief snapshot of Workload as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Workload
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of Workload as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Workload
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

    virtual ~WorkloadAdaptiveMerging() = default;
    WorkloadAdaptiveMerging(const WorkloadAdaptiveMerging&) = default;
    WorkloadAdaptiveMerging& operator=(const WorkloadAdaptiveMerging&) = default;
    WorkloadAdaptiveMerging() = default;
    WorkloadAdaptiveMerging(WorkloadAdaptiveMerging &&) = default;
    WorkloadAdaptiveMerging& operator=(WorkloadAdaptiveMerging &&) = default;

};

#endif