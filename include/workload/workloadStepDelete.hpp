#ifndef WORKLOAD_DELETE_STEP_HPP
#define WORKLOAD_DELETE_STEP_HPP

#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

class WorkloadStepDelete : public WorkloadStep
{
public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToDelete - how many entries pushed to delete operation
     */
    WorkloadStepDelete(DBIndex* index, size_t numEntriesToDelete)
    : WorkloadStep("WorkloadStepDelete", index, numEntriesToDelete, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToDelete - how many entries pushed to delete operation
     */
    WorkloadStepDelete(size_t numEntriesToDelete)
    : WorkloadStep("WorkloadStepDelete", nullptr, numEntriesToDelete, 1, 0.0)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) override
    {
        return new WorkloadStepDelete(*this);
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

        const double time = index->deleteEntries(numOperations);

        finishStep();

        return time;
    }

    virtual ~WorkloadStepDelete() = default;
    WorkloadStepDelete() = default;
    WorkloadStepDelete(const WorkloadStepDelete&) = default;
    WorkloadStepDelete& operator=(const WorkloadStepDelete&) = default;
    WorkloadStepDelete(WorkloadStepDelete &&) = default;
    WorkloadStepDelete& operator=(WorkloadStepDelete &&) = default;
};

#endif