#ifndef WORKLOAD_STEP_HPP
#define WORKLOAD_STEP_HPP

#include <index/dbIndex.hpp>
#include <observability/workloadCounters.hpp>

#include <string>

class WorkloadStep
{
protected:
    const char* name;

    size_t numOperations;
    size_t numEntriesPerOperations; // bulkload, search
    double selectivity; // search

    WorkloadCounters counters;

    DBIndex* index;

    /**
     * @brief Collect counters from Index, Disk, Memory etc
     *
     * @return WorkloadCounters collected from other components
     */
    virtual WorkloadCounters collectCounters() noexcept(true);

    /**
     * @brief Prepare all metdata for step
     *        This method needs to be called before executeStep()
     *
     */
    virtual void prepareStep() noexcept(true);

    /**
     * @brief Finish step (write back all metadata)
     *        This method needs to be called after executeStep()
     *
     */
    virtual void finishStep() noexcept(true);

public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] name - Workload name
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numOperations - how many the same operations will be perform in this step (used in insert, delete, search)
     * @param[in] numEntriesPerOperations - how many entries push into 1 operation (used in bulkload, search)
     * @param[in] selectivity - search selectivity (used only in search)
     */
    WorkloadStep(const char* name, DBIndex* index, size_t numOperations, size_t numEntriesPerOperations, double selectivity);

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] name - Workload name
     * @param[in] numOperations - how many the same operations will be perform in this step (used in insert, delete, search)
     * @param[in] numEntriesPerOperations - how many entries push into 1 operation (used in bulkload, search)
     * @param[in] selectivity - search selectivity (used only in search)
     */
    WorkloadStep(const char* name, size_t numOperations, size_t numEntriesPerOperations, double selectivity);


    /**
     * @brief Set the Db Index object
     *
     * @param[in] index - DBIndex to set into workload
     */
    void setDbIndex(DBIndex* index)
    {
        this->index = index;
    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) = 0;

    /**
     * @brief Created brief snapshot of WorkloadStep as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of WorkloadStep
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of WorkloadStep as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of WorkloadStep
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    /**
     * @brief Execute step like insert, bulkload, search etc
     *
     *
     * @return execution time
     */
    virtual double executeStep() noexcept(true) = 0;

    /**
     * @brief Get the Counters object
     *
     * @return const WorkloadCounters&
     */
    virtual const WorkloadCounters& getCounters() const noexcept(true)
    {
        return counters;
    }

    virtual ~WorkloadStep() = default;
    WorkloadStep() = default;
    WorkloadStep(const WorkloadStep&) = default;
    WorkloadStep& operator=(const WorkloadStep&) = default;
    WorkloadStep(WorkloadStep &&) = default;
    WorkloadStep& operator=(WorkloadStep &&) = default;
};

#endif