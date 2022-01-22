#include <observability/workloadCounters.hpp>

#define TO_STRING_PRIV(X) #X
#define TO_STRING(X) TO_STRING_PRIV(X)

WorkloadCounters::WorkloadCounters()
{
    // add double counters
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_INSERT_AVG_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_INSERT_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_DELETE_AVG_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_DELETE_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_COUNTER_RW_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_COUNTER_RW_TOTAL_TIME)));

    countersDouble.addCounter(WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME)));
    countersDouble.addCounter(WORKLOAD_AM_COUNTER_RW_INVALIDATION_AVG_TIME, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_INVALIDATION_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_AM_COUNTER_RW_LOADING_AVG_TIME, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_LOADING_AVG_TIME)));
    countersDouble.addCounter(WORKLOAD_AM_COUNTER_RW_TOTAL_TIME, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_TOTAL_TIME)));

    // add long counters
    countersLong.addCounter(WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT, std::string(TO_STRING(WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT)));
    countersLong.addCounter(WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT, std::string(TO_STRING(WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)));

    countersLong.addCounter(WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS)));
    countersLong.addCounter(WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS, std::string(TO_STRING(WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS)));

    LOGGER_LOG_DEBUG("Workload counters created {}", toStringFull());
}

void WorkloadCounters::pegCounter(enum WorkloadCountersD counterId, double val) noexcept(true)
{
    countersDouble.pegCounter(counterId, val);
}

void WorkloadCounters::resetCounter(enum WorkloadCountersD counterId) noexcept(true)
{
    countersDouble.resetCounter(counterId);
}

void WorkloadCounters::pegCounter(enum WorkloadCountersL counterId, long val) noexcept(true)
{
    countersLong.pegCounter(counterId, val);
}

void WorkloadCounters::resetCounter(enum WorkloadCountersL counterId) noexcept(true)
{
    countersLong.resetCounter(counterId);
}

double WorkloadCounters::getCounterValue(enum WorkloadCountersD counterId) const noexcept(true)
{
    return countersDouble.getCounterValue(counterId);
}

long WorkloadCounters::getCounterValue(enum WorkloadCountersL counterId) const noexcept(true)
{
    return countersLong.getCounterValue(counterId);
}

std::string WorkloadCounters::getCounterName(enum WorkloadCountersD counterId) const noexcept(true)
{
    return countersDouble.getCounterName(counterId);
}

std::string WorkloadCounters::getCounterName(enum WorkloadCountersL counterId) const noexcept(true)
{
    return countersLong.getCounterName(counterId);
}

std::pair<std::string, double> WorkloadCounters::getCounter(enum WorkloadCountersD counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

std::pair<std::string, long> WorkloadCounters::getCounter(enum WorkloadCountersL counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

void WorkloadCounters::resetAllCounters() noexcept(true)
{
    countersLong.resetAllCounters();
    countersDouble.resetAllCounters();
}

std::string WorkloadCounters::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("WorkloadCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toString() +
                           std::string(" .countersLong = ") + countersLong.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toString() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toString() +  std::string("\n") +
                           std::string("}"));
}

std::string WorkloadCounters::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("WorkloadCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toStringFull() +
                           std::string(" .countersLong = ") + countersLong.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toStringFull() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toStringFull() +  std::string("\n") +
                           std::string("}"));
}
