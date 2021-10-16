#ifndef INDEX_COUNTERS_HPP
#define INDEX_COUNTERS_HPP

#include <observability/counterManager.hpp>

class IndexCounters
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
    enum IndexCountersD : Counters::counterId_t
    {
        INDEX_COUNTER_RW_INSERT_TOTAL_TIME,
        INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME,
        INDEX_COUNTER_RW_DELETE_TOTAL_TIME,
        INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME,
        INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////
        INDEX_COUNTER_RO_INSERT_AVG_TIME,
        INDEX_COUNTER_RO_BULKLOAD_AVG_TIME,
        INDEX_COUNTER_RO_DELETE_AVG_TIME,
        INDEX_COUNTER_RO_PSEARCH_AVG_TIME,
        INDEX_COUNTER_RO_RSEARCH_AVG_TIME,

        INDEX_COUNTER_RO_TOTAL_TIME,

        INDEX_COUNTER_D_MAX_ITERATOR // to iterate over enum
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
    enum IndexCountersL : Counters::counterId_t
    {
        INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS,
        INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS,
        INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS,
        INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS,
        INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////
        INDEX_COUNTER_RO_TOTAL_OPERATIONS,

        INDEX_COUNTER_L_MAX_ITERATOR // to iterate over enum
    };

private:
    CounterManager<double> countersDouble;
    CounterManager<long> countersLong;

    double calculateAvg(enum IndexCountersD total, enum IndexCountersL numberOfOp) const noexcept(true);
    double calculateAvg(enum IndexCountersL total, enum IndexCountersL numberOfOp) const noexcept(true);

public:

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum IndexCountersD counterId, double val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum IndexCountersD counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    double getCounterValue(enum IndexCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum IndexCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getCounter(enum IndexCountersD counterId) const noexcept(true);

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum IndexCountersL counterId, long val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum IndexCountersL counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    long getCounterValue(enum IndexCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum IndexCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getCounter(enum IndexCountersL counterId) const noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllCounters() noexcept(true);

    /**
     * @brief Created brief snapshot of IndexCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of IndexCounters
     */
    std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of IndexCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of IndexCounters
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true);

    IndexCounters();
    ~IndexCounters() = default;
    IndexCounters(const IndexCounters&) = default;
    IndexCounters& operator=(const IndexCounters&) = default;
    IndexCounters(IndexCounters &&) = default;
    IndexCounters& operator=(IndexCounters &&) = default;
};


// operators overloading
inline enum IndexCounters::IndexCountersD& operator++(enum IndexCounters::IndexCountersD& id)
{
    return id = static_cast<enum IndexCounters::IndexCountersD>(static_cast<int>(id) + 1);
}

inline enum IndexCounters::IndexCountersD operator++(enum IndexCounters::IndexCountersD& id, int)
{
    const enum IndexCounters::IndexCountersD temp = id;
    id = static_cast<enum IndexCounters::IndexCountersD>(static_cast<int>(id) + 1);

    return temp;
}

inline enum IndexCounters::IndexCountersD& operator--(enum IndexCounters::IndexCountersD& id)
{
    return id = static_cast<enum IndexCounters::IndexCountersD>(static_cast<int>(id) - 1);
}

inline enum IndexCounters::IndexCountersD operator--(enum IndexCounters::IndexCountersD& id, int)
{
    const enum IndexCounters::IndexCountersD temp = id;
    id = static_cast<enum IndexCounters::IndexCountersD>(static_cast<int>(id) - 1);

    return temp;
}

inline enum IndexCounters::IndexCountersL& operator++(enum IndexCounters::IndexCountersL& id)
{
    return id = static_cast<enum IndexCounters::IndexCountersL>(static_cast<int>(id) + 1);
}

inline enum IndexCounters::IndexCountersL operator++(enum IndexCounters::IndexCountersL& id, int)
{
    const enum IndexCounters::IndexCountersL temp = id;
    id = static_cast<enum IndexCounters::IndexCountersL>(static_cast<int>(id) + 1);

    return temp;
}

inline enum IndexCounters::IndexCountersL& operator--(enum IndexCounters::IndexCountersL& id)
{
    return id = static_cast<enum IndexCounters::IndexCountersL>(static_cast<int>(id) - 1);
}

inline enum IndexCounters::IndexCountersL operator--(enum IndexCounters::IndexCountersL& id, int)
{
    const enum IndexCounters::IndexCountersL temp = id;
    id = static_cast<enum IndexCounters::IndexCountersL>(static_cast<int>(id) - 1);

    return temp;
}


#endif