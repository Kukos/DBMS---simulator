#ifndef COUNTER_MANAGER_HPP
#define COUNTER_MANAGER_HPP

#include <unordered_map>
#include <string>
#include <logger/logger.hpp>

namespace Counters
{
    typedef unsigned long counterId_t;
}

template<typename T>
class CounterManager
{
private:
    std::unordered_map<Counters::counterId_t, std::pair<std::string, T>> counterMap;

public:
    /**
     * @brief add counter to counter manager. There is no start pegging function,
     * when counter is added can be pegged.
     *
     * @param[in] counterId - Counter ID (unique)
     * @param[in] counterName - Counter Name (should be unique but this is not obligatory)
     */
    void addCounter(Counters::counterId_t counterId, const std::string& counterName) noexcept(true)
    {
        // counter already added
        if (counterMap.find(counterId) != counterMap.end())
        {
            LOGGER_LOG_WARN("Counter {} with ID={} already added", counterName, counterId);
            return;
        }

        T counterVal = T();
        std::pair<std::string, T> counterPair = std::make_pair(counterName, counterVal);
        counterMap.insert(std::make_pair(counterId, counterPair));

        LOGGER_LOG_TRACE("Counter {} with ID={} added to counter manager", counterName, counterId);
    }

    /**
     * @brief Default pegging function. Counter is incremented by 1.
     *
     * @param[in] counterId - Counter ID
     */
    void pegCounter(Counters::counterId_t counterId) noexcept(true)
    {
        pegCounter(counterId, static_cast<T>(1));
    }

    /**
     * @brief Increment counter by value @valToPeg
     *
     * @param[in] counterId - Counter ID
     * @param[in] valToPeg - counter will be incremented by this value
     */
    void pegCounter(Counters::counterId_t counterId, T valToPeg) noexcept(true)
    {
        // invalid counterId
        if (counterMap.find(counterId) == counterMap.end())
        {
            LOGGER_LOG_ERROR("Counter with counter ID={} does not exist", counterId);
            return;
        }

        counterMap[counterId].second += valToPeg;

        LOGGER_LOG_TRACE("Counter {} with ID={} pegged with value={}", getCounterName(counterId), counterId, valToPeg);
    }

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    void resetCounter(Counters::counterId_t counterId)
    {
        if (counterMap.find(counterId) == counterMap.end())
        {
            LOGGER_LOG_ERROR("Counter with counter ID={} does not exist", counterId);
            return;
        }

        counterMap[counterId].second = T{0};

        LOGGER_LOG_TRACE("Counter {} with ID={} reseted to value={}", getCounterName(counterId), counterId, T{0});
    }

    /**
     * @brief Reset all counters
     */
    void resetAllCounters()
    {
        for (auto & counter : counterMap)
            counter.second.second = T{0};
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    std::pair<std::string, T> getCounter(Counters::counterId_t counterId) const noexcept(true)
    {
        auto counterIt = counterMap.find(counterId);

        // invalid counterId
        if (counterIt == counterMap.end())
            return std::make_pair("error", T());

        return counterIt->second;
    }

    /**
     * @brief Get Counter Value
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter value
     */
    T getCounterValue(Counters::counterId_t counterId) const noexcept(true)
    {
        return getCounter(counterId).second;
    }

    /**
     * @brief Get Counter Name
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter Name as string
     */
    std::string getCounterName(Counters::counterId_t counterId) const noexcept(true)
    {
        return getCounter(counterId).first;
    }

    /**
     * @brief Created brief snapshot of CounterManager as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of CounterManager
     */
    std::string toString(bool oneLine = true) const noexcept(true)
    {
        std::string str;
        if (oneLine)
        {
            str = std::string("CounterManager = { .counterMap = {");
            for (auto & counter : counterMap)
                str += std::string(" { .id = ") + std::to_string(counter.first) + std::string(" .name = ") + counter.second.first + std::string(" .val = ") + std::to_string(counter.second.second) + std::string("}");

            str += " } }";
        }
        else
        {
            str = std::string("CounterManager = {\n\t.counterMap = {\n");
            for (auto & counter : counterMap)
                str += std::string("\t\t{\n\t\t\t.id = ") + std::to_string(counter.first) + std::string("\n\t\t\t.name = ") + counter.second.first + std::string("\n\t\t\t.val = ") + std::to_string(counter.second.second) + std::string("\n\t\t}\n");

            str += "\t}\n}";
        }

        return str;
    }

    /**
     * @brief Created full snapshot of CounterManager as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of CounterManager
     */
    std::string toStringFull(bool oneLine = true) const noexcept(true)
    {
        return toString(oneLine);
    }

    CounterManager()
    {
        LOGGER_LOG_DEBUG("CounterManager created {}", toStringFull());
    }

    ~CounterManager() = default;
    CounterManager(const CounterManager&) = default;
    CounterManager& operator=(const CounterManager&) = default;
    CounterManager(CounterManager &&) = default;
    CounterManager& operator=(CounterManager &&) = default;
};

#endif