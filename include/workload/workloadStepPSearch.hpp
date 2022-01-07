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
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     */
    WorkloadStepPSearch(DBIndexColumn* index, size_t numEntriesToPSearch)
    : WorkloadStep("WorkloadStepPointSearch", index, numEntriesToPSearch, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     * @param[in] col - columns to search
     */
    WorkloadStepPSearch(DBIndexColumn* index, size_t numEntriesToPSearch, const std::vector<size_t>& col)
    : WorkloadStep("WorkloadStepPointSearch", index, numEntriesToPSearch, 1, 0.0, col)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     */
    WorkloadStepPSearch(size_t numEntriesToPSearch)
    : WorkloadStep("WorkloadStepPointSearch", static_cast<DBIndex*>(nullptr), numEntriesToPSearch, 1, 0.0)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] numEntriesToPSearch - how many entries pushed to search operation
     * @param[in] col - columns to search
     */
    WorkloadStepPSearch(size_t numEntriesToPSearch, const std::vector<size_t>& col)
    : WorkloadStep("WorkloadStepPointSearch", static_cast<DBIndexColumn*>(nullptr), numEntriesToPSearch, 1, 0.0, col)
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
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(DBIndexColumn* index, double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch", index, numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] index - pointer to index (wont be deallocated)
     * @param[in] selectivity - search selectivity
     * @param[in] col - columns to search
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(DBIndexColumn* index, double selectivity, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch", index, numOperations, 0, selectivity, col)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] selectivity - search selectivity
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(double selectivity, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch",  static_cast<DBIndex*>(nullptr), numOperations, 0, selectivity)
    {

    }

    /**
     * @brief Construct a new Workload Step object
     *
     * @param[in] selectivity - search selectivity
     * @param[in] col - columns to search
     * @param[in] numOperations - how many operations perfromed (default 1)
     */
    WorkloadStepPSearch(double selectivity, const std::vector<size_t>& col, size_t numOperations = 1)
    : WorkloadStep("WorkloadStepPointSearch",  static_cast<DBIndexColumn*>(nullptr), numOperations, 0, selectivity, col)
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
                time = rIndex->findPointEntries(selectivity, numOperations);
            else
                time = rIndex->findPointEntries(numOperations);
        }
        else
        {
            if (numEntriesPerOperations == 0) // selectivity is non-zero
                time = cIndex->findPointEntries(columnsToSearch, selectivity, numOperations);
            else
                time = cIndex->findPointEntries(columnsToSearch, numOperations);
        }

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