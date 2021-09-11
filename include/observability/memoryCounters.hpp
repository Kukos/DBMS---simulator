#ifndef MEMORY_COUNTERS_HPP
#define MEMORY_COUNTERS_HPP

#include <observability/counterManager.hpp>

class MemoryCounters
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
    enum MemoryCountersD : Counters::counterId_t
    {
        MEMORY_COUNTER_RW_READ_TOTAL_TIME,
        MEMORY_COUNTER_RW_WRITE_TOTAL_TIME,
        MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////

        MEMORY_COUNTER_RO_READ_AVG_TIME,
        MEMORY_COUNTER_RO_WRITE_AVG_TIME,
        MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME,

        MEMORY_COUNTER_RO_READ_AVG_BYTES,
        MEMORY_COUNTER_RO_WRITE_AVG_BYTES,
        MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES,

        MEMORY_COUNTER_RO_TOTAL_TIME,

        MEMORY_COUNTER_D_MAX_ITERATOR // to iterate over enum
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
    enum MemoryCountersL : Counters::counterId_t
    {
        MEMORY_COUNTER_RW_READ_TOTAL_BYTES,
        MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES,
        MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES,

        MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS,
        MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS,
        MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS,

        //////// READ ONLY COUNTERS, BASED ON RW COUNTERS ////////

        MEMORY_COUNTER_RO_TOTAL_BYTES,
        MEMORY_COUNTER_RO_TOTAL_OPERATIONS,

        MEMORY_COUNTER_L_MAX_ITERATOR // to iterate over enum
    };

private:
    CounterManager<double> countersDouble;
    CounterManager<long> countersLong;

    double calculateAvg(enum MemoryCountersD total, enum MemoryCountersL numberOfOp) const noexcept(true);
    double calculateAvg(enum MemoryCountersL total, enum MemoryCountersL numberOfOp) const noexcept(true);

public:

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum MemoryCountersD counterId, double val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum MemoryCountersD counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    double getCounterValue(enum MemoryCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum MemoryCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, double> getCounter(enum MemoryCountersD counterId) const noexcept(true);

    /**
     * @brief Increment counter by value @val
     *
     * @param[in] counterId - Counter ID
     * @param[in] val - counter will be incremented by this value
     */
    void pegCounter(enum MemoryCountersL counterId, long val) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(enum MemoryCountersL counterId) noexcept(true);

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    long getCounterValue(enum MemoryCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(enum MemoryCountersL counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, long> getCounter(enum MemoryCountersL counterId) const noexcept(true);

    /**
     * @brief Reset all counters
     */
    void resetAllCounters() noexcept(true);

    /**
     * @brief Created brief snapshot of MemoryCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of MemoryCounters
     */
    std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of MemoryCounters as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of MemoryCounters
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true);

    MemoryCounters();
    ~MemoryCounters() = default;
    MemoryCounters(const MemoryCounters&) = default;
    MemoryCounters& operator=(const MemoryCounters&) = default;
    MemoryCounters(MemoryCounters &&) = default;
    MemoryCounters& operator=(MemoryCounters &&) = default;
};


// operators overloading
inline enum MemoryCounters::MemoryCountersD& operator++(enum MemoryCounters::MemoryCountersD& id)
{
    return id = static_cast<enum MemoryCounters::MemoryCountersD>(static_cast<int>(id) + 1);
}

inline enum MemoryCounters::MemoryCountersD operator++(enum MemoryCounters::MemoryCountersD& id, int)
{
    const enum MemoryCounters::MemoryCountersD temp = id;
    id = static_cast<enum MemoryCounters::MemoryCountersD>(static_cast<int>(id) + 1);

    return temp;
}

inline enum MemoryCounters::MemoryCountersD& operator--(enum MemoryCounters::MemoryCountersD& id)
{
    return id = static_cast<enum MemoryCounters::MemoryCountersD>(static_cast<int>(id) - 1);
}

inline enum MemoryCounters::MemoryCountersD operator--(enum MemoryCounters::MemoryCountersD& id, int)
{
    const enum MemoryCounters::MemoryCountersD temp = id;
    id = static_cast<enum MemoryCounters::MemoryCountersD>(static_cast<int>(id) - 1);

    return temp;
}

inline enum MemoryCounters::MemoryCountersL& operator++(enum MemoryCounters::MemoryCountersL& id)
{
    return id = static_cast<enum MemoryCounters::MemoryCountersL>(static_cast<int>(id) + 1);
}

inline enum MemoryCounters::MemoryCountersL operator++(enum MemoryCounters::MemoryCountersL& id, int)
{
    const enum MemoryCounters::MemoryCountersL temp = id;
    id = static_cast<enum MemoryCounters::MemoryCountersL>(static_cast<int>(id) + 1);

    return temp;
}

inline enum MemoryCounters::MemoryCountersL& operator--(enum MemoryCounters::MemoryCountersL& id)
{
    return id = static_cast<enum MemoryCounters::MemoryCountersL>(static_cast<int>(id) - 1);
}

inline enum MemoryCounters::MemoryCountersL operator--(enum MemoryCounters::MemoryCountersL& id, int)
{
    const enum MemoryCounters::MemoryCountersL temp = id;
    id = static_cast<enum MemoryCounters::MemoryCountersL>(static_cast<int>(id) - 1);

    return temp;
}

#endif