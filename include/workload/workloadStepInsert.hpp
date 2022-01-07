#ifndef WORKLOAD_INSERT_STEP_HPP
#define WORKLOAD_INSERT_STEP_HPP

#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

class WorkloadStepInsert : public WorkloadStep
{
public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToInsert - how many entries pushed to insert operation
     */
    WorkloadStepInsert(DBIndex* index, size_t numEntriesToInsert)
    : WorkloadStep("WorkloadStepInsert", index, numEntriesToInsert, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToInsert - how many entries pushed to insert operation
     */
    WorkloadStepInsert(DBIndexColumn* index, size_t numEntriesToInsert)
    : WorkloadStep("WorkloadStepInsert", index, numEntriesToInsert, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToInsert - how many entries pushed to insert operation
     */
    WorkloadStepInsert(size_t numEntriesToInsert)
    : WorkloadStep("WorkloadStepInsert", static_cast<DBIndex*>(nullptr), numEntriesToInsert, 1, 0.0)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) override
    {
        return new WorkloadStepInsert(*this);
    }

    /**
     * @brief Execute step like insert, bulkload, search etc
     *
     *
     * @return execution time
     */
    virtual double executeStep() noexcept(true) override
    {
        if ((isColumnIndexMode == false && rIndex == nullptr) || (isColumnIndexMode == true && cIndex == nullptr))
        {
            LOGGER_LOG_ERROR("Index is not set (nullptr)");
            return 0.0;
        }

        prepareStep();

        const double time = isColumnIndexMode == false ? rIndex->insertEntries(numOperations) : cIndex->insertEntries(numOperations);

        finishStep();

        return time;
    }

    virtual ~WorkloadStepInsert() = default;
    WorkloadStepInsert() = default;
    WorkloadStepInsert(const WorkloadStepInsert&) = default;
    WorkloadStepInsert& operator=(const WorkloadStepInsert&) = default;
    WorkloadStepInsert(WorkloadStepInsert &&) = default;
    WorkloadStepInsert& operator=(WorkloadStepInsert &&) = default;
};

#endif