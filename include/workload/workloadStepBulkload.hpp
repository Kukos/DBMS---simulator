#ifndef WORKLOAD_BULKLOAD_STEP_HPP
#define WORKLOAD_BULKLOAD_STEP_HPP

#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

class WorkloadStepBulkload : public WorkloadStep
{
public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToBulkload - how many entries pushed to bulkload operation
     */
    WorkloadStepBulkload(DBIndex* index, size_t numEntriesToBulkload)
    : WorkloadStep("WorkloadStepBulkload", index, 1, numEntriesToBulkload, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToBulkload - how many entries pushed to bulkload operation
     */
    WorkloadStepBulkload(size_t numEntriesToBulkload)
    : WorkloadStep("WorkloadStepBulkload", nullptr, 1, numEntriesToBulkload, 0.0)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) override
    {
        return new WorkloadStepBulkload(*this);
    }

    /**
     * @brief Execute step like insert, bulkload, search etc
     *
     *
     * @return execution time
     */
    virtual double executeStep() noexcept(true) override
    {
        if (index == nullptr)
        {
            LOGGER_LOG_ERROR("Index is not set (nullptr)");
            return 0.0;
        }

        prepareStep();

        double time = 0.0;
        if (index->isBulkloadSupported())
            time = index->bulkloadEntries(numEntriesPerOperations);
        else
            time = index->insertEntries(numEntriesPerOperations);

        finishStep();

        return time;
    }

    virtual ~WorkloadStepBulkload() = default;
    WorkloadStepBulkload() = default;
    WorkloadStepBulkload(const WorkloadStepBulkload&) = default;
    WorkloadStepBulkload& operator=(const WorkloadStepBulkload&) = default;
    WorkloadStepBulkload(WorkloadStepBulkload &&) = default;
    WorkloadStepBulkload& operator=(WorkloadStepBulkload &&) = default;
};

#endif