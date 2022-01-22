#include <observability/adaptiveMergingCounters.hpp>

#define TO_STRING_PRIV(X) #X
#define TO_STRING(X) TO_STRING_PRIV(X)

double AdaptiveMergingCounters::calculateAvg(enum AdaptiveMergingCountersD total, enum AdaptiveMergingCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const double totalCost = countersDouble.getCounterValue(total);

    if (numOfOperation == 0)
        return  0.0;

    return totalCost / static_cast<double>(numOfOperation);
}

double AdaptiveMergingCounters::calculateAvg(enum AdaptiveMergingCountersL total, enum AdaptiveMergingCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const long totalCost = countersLong.getCounterValue(total);

    if (numOfOperation == 0)
        return 0.0;

    return  static_cast<double>(totalCost) / static_cast<double>(numOfOperation);
}

AdaptiveMergingCounters::AdaptiveMergingCounters()
{
    // add double counters
    countersDouble.addCounter(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME)));
    countersDouble.addCounter(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME)));


    // add RO counters to get name in easy way
    countersDouble.addCounter(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME)));
    countersDouble.addCounter(ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME)));
    countersDouble.addCounter(ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME)));

    // add long counters
    countersLong.addCounter(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS)));
    countersLong.addCounter(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS)));

    // add RO counters to get name is easy way
    countersLong.addCounter(ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS, std::string(TO_STRING(ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS)));

    LOGGER_LOG_DEBUG("Index counters created {}", toStringFull());
}

void AdaptiveMergingCounters::pegCounter(enum AdaptiveMergingCountersD counterId, double val) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME)
        return; // cannot peg RO counter

    countersDouble.pegCounter(counterId, val);
}

void AdaptiveMergingCounters::resetCounter(enum AdaptiveMergingCountersD counterId) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME)
        return; // cannot reset RO counter

    countersDouble.resetCounter(counterId);
}

void AdaptiveMergingCounters::pegCounter(enum AdaptiveMergingCountersL counterId, long val) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS)
        return; // cannot peg RO counter

    countersLong.pegCounter(counterId, val);
}

void AdaptiveMergingCounters::resetCounter(enum AdaptiveMergingCountersL counterId) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS)
        return; // cannot reset RO counter

    countersLong.resetCounter(counterId);
}

double AdaptiveMergingCounters::getCounterValue(enum AdaptiveMergingCountersD counterId) const noexcept(true)
{
    // rw counter
    if (counterId < ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME)
        return countersDouble.getCounterValue(counterId);

    // RO counters need special calculation
    double counterVal = 0.0;
    switch (counterId)
    {
        case ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_AVG_TIME:
        {
            counterVal = calculateAvg(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME, ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS);
            break;
        }
        case ADAPTIVE_MERGING_COUNTER_RW_LOADING_AVG_TIME:
        {
            counterVal = calculateAvg(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME, ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);
            break;
        }
        case ADAPTIVE_MERGING_COUNTER_RO_TOTAL_TIME:
        {
            counterVal = countersDouble.getCounterValue(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_TIME) +
                         countersDouble.getCounterValue(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_TIME);
            break;
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

long AdaptiveMergingCounters::getCounterValue(enum AdaptiveMergingCountersL counterId) const noexcept(true)
{
    // rw counter
    if (counterId < ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS)
        return countersLong.getCounterValue(counterId);

    // RO counters need special calculation
    long counterVal = 0.0;
    switch (counterId)
    {
        case ADAPTIVE_MERGING_COUNTER_RO_TOTAL_OPERATIONS:
        {
            counterVal = countersLong.getCounterValue(ADAPTIVE_MERGING_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(ADAPTIVE_MERGING_COUNTER_RW_LOADING_TOTAL_OPERATIONS);
            break;
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

std::string AdaptiveMergingCounters::getCounterName(enum AdaptiveMergingCountersD counterId) const noexcept(true)
{
    return countersDouble.getCounterName(counterId);
}

std::string AdaptiveMergingCounters::getCounterName(enum AdaptiveMergingCountersL counterId) const noexcept(true)
{
    return countersLong.getCounterName(counterId);
}

std::pair<std::string, double> AdaptiveMergingCounters::getCounter(enum AdaptiveMergingCountersD counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

std::pair<std::string, long> AdaptiveMergingCounters::getCounter(enum AdaptiveMergingCountersL counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

void AdaptiveMergingCounters::resetAllCounters() noexcept(true)
{
    countersLong.resetAllCounters();
    countersDouble.resetAllCounters();
}

std::string AdaptiveMergingCounters::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMergingCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toString() +
                           std::string(" .countersLong = ") + countersLong.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMergingCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toString() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toString() +  std::string("\n") +
                           std::string("}"));
}

std::string AdaptiveMergingCounters::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("AdaptiveMergingCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toStringFull() +
                           std::string(" .countersLong = ") + countersLong.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("AdaptiveMergingCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toStringFull() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toStringFull() +  std::string("\n") +
                           std::string("}"));
}
