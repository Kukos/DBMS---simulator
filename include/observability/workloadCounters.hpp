#ifndef WORKLOAD_COUNTERS_HPP
#define WORKLOAD_COUNTERS_HPP

#include <observability/counterManager.hpp>

class WorkloadCounters
{
public:
    /**
     * This enum describes counters ID.
     * Each counter stores value as a double.
     *
     * RW in counter name means counter is read / write.
     * So you can peg this by using public method.
     *
     * RO in counter name means counter is read only.
     * This means that counter value is calculated from other counters.
     * You cannot peg this value by using public method.
     * You can only get counter / value/ name of this type of counter
     */
    enum WorkloadCountersD : Counters::counterId_t
    {
        WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME,
        WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME,
        WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME,
        WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME,
        WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME,
        WORKLOAD_COUNTER_RW_INSERT_AVG_TIME,
        WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME,
        WORKLOAD_COUNTER_RW_DELETE_AVG_TIME,
        WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME,
        WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME,
        WORKLOAD_COUNTER_RW_TOTAL_TIME,

        WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME,
        WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME,
        WORKLOAD_AM_COUNTER_RW_INVALIDATION_AVG_TIME,
        WORKLOAD_AM_COUNTER_RW_LOADING_AVG_TIME,
        WORKLOAD_AM_COUNTER_RW_TOTAL_TIME,

        WORKLOAD_COUNTER_D_MAX_ITERATOR // to iterate over enum
    };

    /**
     * This enum describes counters ID.
     * Each counter stores value as a long.
     *
     * RW in counter name means counter is read / write.
     * So you can peg this by using public method.
     *
     * RO in counter name means counter is read only.
     * This means that counter value is calculated from other counters.
     * You cannot peg this value by using public method.
     * You can only get counter / value/ name of this type of counter
     */
    enum WorkloadCountersL : Counters::counterId_t
    {
        WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS,
        WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT,
        WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT,

        WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS,
        WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS,
        WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS,

        WORKLOAD_COUNTER_L_MAX_ITERATOR // to iterate over enum
    };

private:
    CounterManager<double> countersDouble;
    CounterManager<long> countersLong;

    double calculateAvg(enum WorkloadCountersD total, enum WorkloadCountersL numberOfOp) const noexcept(true);
    double calculateAvg(enum WorkloadCountersL total, enum WorkloadCountersL numberOfOp) const noexcept(true);

public:

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum WorkloadCountersD counterId, double val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum WorkloadCountersD counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    double getCounterValue(enum WorkloadCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum WorkloadCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getCounter(enum WorkloadCountersD counterId) const noexcept(true);

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum WorkloadCountersL counterId, long val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum WorkloadCountersL counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    long getCounterValue(enum WorkloadCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum WorkloadCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getCounter(enum WorkloadCountersL counterId) const noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllCounters() noexcept(true);

    /**
     * @brief Created brief snapshot of WorkloadCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of WorkloadCounters
     */
    std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of WorkloadCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of WorkloadCounters
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true);

    WorkloadCounters();
    ~WorkloadCounters() = default;
    WorkloadCounters(const WorkloadCounters&) = default;
    WorkloadCounters& operator=(const WorkloadCounters&) = default;
    WorkloadCounters(WorkloadCounters &&) = default;
    WorkloadCounters& operator=(WorkloadCounters &&) = default;
};


// operators overloading
inline enum WorkloadCounters::WorkloadCountersD& operator++(enum WorkloadCounters::WorkloadCountersD& id)
{
    return id = static_cast<enum WorkloadCounters::WorkloadCountersD>(static_cast<int>(id) + 1);
}

inline enum WorkloadCounters::WorkloadCountersD operator++(enum WorkloadCounters::WorkloadCountersD& id, int)
{
    const enum WorkloadCounters::WorkloadCountersD temp = id;
    id = static_cast<enum WorkloadCounters::WorkloadCountersD>(static_cast<int>(id) + 1);

    return temp;
}

inline enum WorkloadCounters::WorkloadCountersD& operator--(enum WorkloadCounters::WorkloadCountersD& id)
{
    return id = static_cast<enum WorkloadCounters::WorkloadCountersD>(static_cast<int>(id) - 1);
}

inline enum WorkloadCounters::WorkloadCountersD operator--(enum WorkloadCounters::WorkloadCountersD& id, int)
{
    const enum WorkloadCounters::WorkloadCountersD temp = id;
    id = static_cast<enum WorkloadCounters::WorkloadCountersD>(static_cast<int>(id) - 1);

    return temp;
}

inline enum WorkloadCounters::WorkloadCountersL& operator++(enum WorkloadCounters::WorkloadCountersL& id)
{
    return id = static_cast<enum WorkloadCounters::WorkloadCountersL>(static_cast<int>(id) + 1);
}

inline enum WorkloadCounters::WorkloadCountersL operator++(enum WorkloadCounters::WorkloadCountersL& id, int)
{
    const enum WorkloadCounters::WorkloadCountersL temp = id;
    id = static_cast<enum WorkloadCounters::WorkloadCountersL>(static_cast<int>(id) + 1);

    return temp;
}

inline enum WorkloadCounters::WorkloadCountersL& operator--(enum WorkloadCounters::WorkloadCountersL& id)
{
    return id = static_cast<enum WorkloadCounters::WorkloadCountersL>(static_cast<int>(id) - 1);
}

inline enum WorkloadCounters::WorkloadCountersL operator--(enum WorkloadCounters::WorkloadCountersL& id, int)
{
    const enum WorkloadCounters::WorkloadCountersL temp = id;
    id = static_cast<enum WorkloadCounters::WorkloadCountersL>(static_cast<int>(id) - 1);

    return temp;
}


#endif