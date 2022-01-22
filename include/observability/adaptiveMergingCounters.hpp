#ifndef ADAPTIVE_MERGING_COUNTERS_HPP
#define ADAPTIVE_MERGING_COUNTERS_HPP

#include <observability/counterManager.hpp>

class AdaptiveMergingCounters
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
    enum AdaptiveMergingCountersD : Counters::counterId_t
    {
        ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME,
        ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////
        ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME,
        ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME,

        ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME,

        ADAPTIVE_MERGING_COUNTER_D_MAX_ITERATOR // to iterate over enum
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
    enum AdaptiveMergingCountersL : Counters::counterId_t
    {
        ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS,
        ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////
        ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS,

        ADAPTIVE_MERGING_COUNTER_L_MAX_ITERATOR // to iterate over enum
    };

private:
    CounterManager<double> countersDouble;
    CounterManager<long> countersLong;

    double calculateAvg(enum AdaptiveMergingCountersD total, enum AdaptiveMergingCountersL numberOfOp) const noexcept(true);
    double calculateAvg(enum AdaptiveMergingCountersL total, enum AdaptiveMergingCountersL numberOfOp) const noexcept(true);

public:

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum AdaptiveMergingCountersD counterId, double val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum AdaptiveMergingCountersD counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    double getCounterValue(enum AdaptiveMergingCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum AdaptiveMergingCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getCounter(enum AdaptiveMergingCountersD counterId) const noexcept(true);

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum AdaptiveMergingCountersL counterId, long val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum AdaptiveMergingCountersL counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    long getCounterValue(enum AdaptiveMergingCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum AdaptiveMergingCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getCounter(enum AdaptiveMergingCountersL counterId) const noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllCounters() noexcept(true);

    /**
     * @brief Created brief snapshot of AdaptiveMergingCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of AdaptiveMergingCounters
     */
    std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of AdaptiveMergingCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of AdaptiveMergingCounters
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true);

    AdaptiveMergingCounters();
    ~AdaptiveMergingCounters() = default;
    AdaptiveMergingCounters(const AdaptiveMergingCounters&) = default;
    AdaptiveMergingCounters& operator=(const AdaptiveMergingCounters&) = default;
    AdaptiveMergingCounters(AdaptiveMergingCounters &&) = default;
    AdaptiveMergingCounters& operator=(AdaptiveMergingCounters &&) = default;
};


// operators overloading
inline enum AdaptiveMergingCounters::AdaptiveMergingCountersD& operator++(enum AdaptiveMergingCounters::AdaptiveMergingCountersD& id)
{
    return id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersD>(static_cast<int>(id) + 1);
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersD operator++(enum AdaptiveMergingCounters::AdaptiveMergingCountersD& id, int)
{
    const enum AdaptiveMergingCounters::AdaptiveMergingCountersD temp = id;
    id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersD>(static_cast<int>(id) + 1);

    return temp;
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersD& operator--(enum AdaptiveMergingCounters::AdaptiveMergingCountersD& id)
{
    return id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersD>(static_cast<int>(id) - 1);
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersD operator--(enum AdaptiveMergingCounters::AdaptiveMergingCountersD& id, int)
{
    const enum AdaptiveMergingCounters::AdaptiveMergingCountersD temp = id;
    id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersD>(static_cast<int>(id) - 1);

    return temp;
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersL& operator++(enum AdaptiveMergingCounters::AdaptiveMergingCountersL& id)
{
    return id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersL>(static_cast<int>(id) + 1);
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersL operator++(enum AdaptiveMergingCounters::AdaptiveMergingCountersL& id, int)
{
    const enum AdaptiveMergingCounters::AdaptiveMergingCountersL temp = id;
    id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersL>(static_cast<int>(id) + 1);

    return temp;
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersL& operator--(enum AdaptiveMergingCounters::AdaptiveMergingCountersL& id)
{
    return id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersL>(static_cast<int>(id) - 1);
}

inline enum AdaptiveMergingCounters::AdaptiveMergingCountersL operator--(enum AdaptiveMergingCounters::AdaptiveMergingCountersL& id, int)
{
    const enum AdaptiveMergingCounters::AdaptiveMergingCountersL temp = id;
    id = static_cast<enum AdaptiveMergingCounters::AdaptiveMergingCountersL>(static_cast<int>(id) - 1);

    return temp;
}


#endif