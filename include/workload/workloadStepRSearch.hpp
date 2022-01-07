#ifndef WORKLOAD_RSEARCH_STEP_HPP
#define WORKLOAD_RSEARCH_STEP_HPP

#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

class WorkloadStepRSearch : public WorkloadStep
{
public:
    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToRSearch - how many entries find in 1 operation
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndex* index, size_t numEntriesToRSearch, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, numEntriesToRSearch, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToRSearch - how many entries find in 1 operation
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndexColumn* index, size_t numEntriesToRSearch, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, numEntriesToRSearch, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToRSearch - how many entries find in 1 operation
     * @param[in] col - columns to search
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndexColumn* index, size_t numEntriesToRSearch, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, numEntriesToRSearch, 0.0, col)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToRSearch - how many entries find in 1 operation
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(size_t numEntriesToRSearch, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", static_cast<DBIndex*>(nullptr), numOperations, numEntriesToRSearch, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToRSearch - how many entries find in 1 operation
     * @param[in] col - columns to search
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(size_t numEntriesToRSearch, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", static_cast<DBIndexColumn*>(nullptr), numOperations, numEntriesToRSearch, 0.0, col)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndex* index, double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndexColumn* index, double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] col - columns to search
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(DBIndexColumn* index, double selectivity, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", index, numOperations, 0, selectivity, col)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", static_cast<DBIndex*>(nullptr), numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] selectivity - search selectivity
     * @param[in] col - columns to search
     * @param[in] numOperations - how many search to perform
     */
    WorkloadStepRSearch(double selectivity, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepRangeSearch", static_cast<DBIndexColumn*>(nullptr), numOperations, 0, selectivity, col)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadStep
    *
    * @return new WorkloadStep
    */
    virtual WorkloadStep* clone() const noexcept(true) override
    {
        return new WorkloadStepRSearch(*this);
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

        double time = 0.0;

        if (isColumnIndexMode == false)
        {
            if (numEntriesPerOperations == 0) // selectivity is non-zero
                time = rIndex->findRangeEntries(selectivity, numOperations);
            else
                time = rIndex->findRangeEntries(numEntriesPerOperations, numOperations);
        }
        else
        {
            if (numEntriesPerOperations == 0) // selectivity is non-zero
                time = cIndex->findRangeEntries(columnsToSearch, selectivity, numOperations);
            else
                time = cIndex->findRangeEntries(columnsToSearch, numEntriesPerOperations, numOperations);
        }

        finishStep();

        return time;
    }

    virtual ~WorkloadStepRSearch() = default;
    WorkloadStepRSearch() = default;
    WorkloadStepRSearch(const WorkloadStepRSearch&) = default;
    WorkloadStepRSearch& operator=(const WorkloadStepRSearch&) = default;
    WorkloadStepRSearch(WorkloadStepRSearch &&) = default;
    WorkloadStepRSearch& operator=(WorkloadStepRSearch &&) = default;
};

#endif