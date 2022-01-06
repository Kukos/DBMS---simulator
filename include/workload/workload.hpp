#ifndef WORKLOAD_HPP
#define WORKLOAD_HPP

#include <observability/workloadCounters.hpp>
#include <index/dbIndex.hpp>
#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

#include <vector>

class Workload
{
protected:
    std::vector<WorkloadStep*> steps;

    // per Index
    std::vector<DBIndex*> indexes;
    std::vector<WorkloadCounters> totalCounters;
    std::vector<std::vector<WorkloadCounters>> stepCounters;

    void aggregateCounters(WorkloadCounters& total, const WorkloadCounters& step) noexcept(true);
public:

    /**
     * @brief Construct a new Workload object
     *
     * @param[in] indexes - vector of Indexes
     * @param[in] steps  - vector of Workload steps
     */
    Workload(const std::vector<DBIndex*>& indexes, const std::vector<WorkloadStep*>& steps);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new Workload
    *
    * @return new Workload
    */
    virtual Workload* clone() const noexcept(true)
    {
        return new Workload(*this);
    }

    /**
     * @brief Add Workload Step to the Workload
     *
     * @param[in] step - step to add
     */
    void addStep(WorkloadStep* step) noexcept(true);

    /**
     * @brief Add Index to the Workload
     *
     * @param[in] index - index to add
     */
    void addIndex(DBIndex* index) noexcept(true);

    /**
     * @brief Run all steps for all indexes
     *
     */
    virtual void run() noexcept(true);

    /**
     * @brief Get the Num Indexes in Workload
     *
     * @return number of indexes in workload
     */
    size_t getNumIndexes() const noexcept(true)
    {
        return indexes.size();
    }

    /**
     * @brief Get the All Indexes
     *
     * @return vector with all indexes
     */
    const std::vector<DBIndex*>& getAllIndexes() const noexcept(true)
    {
        return indexes;
    }

    /**
     * @brief Get the Num Steps in workload
     *
     *
     * @return number of steps in workload
     */
    size_t getNumSteps() const noexcept(true)
    {
        return steps.size();
    }

    /**
     * @brief Get the Total Counters object
     *
     * @param[in] index - index of DBIndex in vector
     * @return total Counters of DBIndex[index]
     */
    const WorkloadCounters& getTotalCounters(size_t index) const noexcept(true)
    {
        if (index >= totalCounters.size())
            LOGGER_LOG_ERROR("Index {} >= vector size {}", index, totalCounters.size());

        return totalCounters[index];
    }

    /**
     * @brief Get the All Total Counters
     *
     * @return vector with all total counters
     */
    const std::vector<WorkloadCounters>& getAllTotalCounters() const noexcept(true)
    {
        return totalCounters;
    }

    /**
     * @brief Get the Step Counters object
     *
     * @param[in] index - index of DBIndex in vector
     * @param[in] step - index of step in vector
     * @return stepCounters[index][step]
     */
    const WorkloadCounters& getStepCounters(size_t index, size_t step) const noexcept(true)
    {
        if (index >= stepCounters.size())
            LOGGER_LOG_ERROR("Index {} >= vector size {}", index, stepCounters.size());

        if (step >= stepCounters[index].size())
            LOGGER_LOG_ERROR("Step {} >= vector size {}", step, stepCounters[index].size());

        return stepCounters[index][step];
    }

    /**
     * @brief Get the All Step Counters
     *
     * @return vector with all step counters
     */
    const std::vector<std::vector<WorkloadCounters>>& getAllStepCounters() const noexcept(true)
    {
        return stepCounters;
    }

    /**
     * @brief Created brief snapshot of Workload as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of Workload
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of Workload as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of Workload
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    virtual ~Workload();
    Workload(const Workload&);
    Workload& operator=(const Workload&);

    Workload() = default;
    Workload(Workload &&) = default;
    Workload& operator=(Workload &&) = default;
};

#endif