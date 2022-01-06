#ifndef WORKLOAD_PSEARCH_STEP_HPP
#define WORKLOAD_PSEARCH_STEP_HPP

#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

class WorkloadStepPSearch : public WorkloadStep
{
public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     */
    WorkloadStepPSearch(DBIndex* index, size_t numEntriesToPSearch)
    : WorkloadStep("WorkloadStepPointSearch", index, numEntriesToPSearch, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     */
    WorkloadStepPSearch(size_t numEntriesToPSearch)
    : WorkloadStep("WorkloadStepPointSearch", nullptr, numEntriesToPSearch, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(DBIndex* index, double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch", index, numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch", nullptr, numOperations, 0, selectivity)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) override
    {
        return new WorkloadStepPSearch(*this);
    }


    /**
     * @brief Execute step like insert, bulkload, search etc
     *
     *
     * @return execution time
     */
    virtual double executeStep() noexcept(true)
    {
        if (index == nullptr)
        {
            LOGGER_LOG_ERROR("Index is not set (nullptr)");
            return 0.0;
        }

        prepareStep();

        double time = 0.0;

        if (numEntriesPerOperations == 0) // selectivity is non-zero
            time = index->findPointEntries(selectivity, numOperations);
        else
            time = index->findPointEntries(numOperations);

        finishStep();

        return time;
    }

    virtual ~WorkloadStepPSearch() = default;
    WorkloadStepPSearch() = default;
    WorkloadStepPSearch(const WorkloadStepPSearch&) = default;
    WorkloadStepPSearch& operator=(const WorkloadStepPSearch&) = default;
    WorkloadStepPSearch(WorkloadStepPSearch &&) = default;
    WorkloadStepPSearch& operator=(WorkloadStepPSearch &&) = default;
};

#endif