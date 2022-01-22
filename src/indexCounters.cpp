#include <observability/indexCounters.hpp>

#define TO_STRING_PRIV(X) #X
#define TO_STRING(X) TO_STRING_PRIV(X)

double IndexCounters::calculateAvg(enum IndexCountersD total, enum IndexCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const double totalCost = countersDouble.getCounterValue(total);

    if (numOfOperation == 0)
        return  0.0;

    return totalCost / static_cast<double>(numOfOperation);
}

double IndexCounters::calculateAvg(enum IndexCountersL total, enum IndexCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const long totalCost = countersLong.getCounterValue(total);

    if (numOfOperation == 0)
        return 0.0;

    return  static_cast<double>(totalCost) / static_cast<double>(numOfOperation);
}

IndexCounters::IndexCounters()
{
    // add double counters
    countersDouble.addCounter(INDEX_COUNTER_RW_INSERT_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RW_INSERT_TOTAL_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RW_DELETE_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RW_DELETE_TOTAL_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME)));


    // add RO counters to get name in easy way
    countersDouble.addCounter(INDEX_COUNTER_RO_INSERT_AVG_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_INSERT_AVG_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RO_BULKLOAD_AVG_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_BULKLOAD_AVG_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RO_DELETE_AVG_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_DELETE_AVG_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RO_PSEARCH_AVG_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_PSEARCH_AVG_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RO_RSEARCH_AVG_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_RSEARCH_AVG_TIME)));
    countersDouble.addCounter(INDEX_COUNTER_RO_TOTAL_TIME, std::string(TO_STRING(INDEX_COUNTER_RO_TOTAL_TIME)));

    // add long counters
    countersLong.addCounter(INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS)));
    countersLong.addCounter(INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS)));
    countersLong.addCounter(INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS)));
    countersLong.addCounter(INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS)));
    countersLong.addCounter(INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS)));

    // add RO counters to get name is easy way
    countersLong.addCounter(INDEX_COUNTER_RO_TOTAL_OPERATIONS, std::string(TO_STRING(INDEX_COUNTER_RO_TOTAL_OPERATIONS)));

    LOGGER_LOG_DEBUG("Index counters created {}", toStringFull());
}

void IndexCounters::pegCounter(enum IndexCountersD counterId, double val) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= INDEX_COUNTER_RO_INSERT_AVG_TIME)
        return; // cannot peg RO counter

    countersDouble.pegCounter(counterId, val);
}

void IndexCounters::resetCounter(enum IndexCountersD counterId) noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_D_MAX_ITERATOR)
        return;

    // RO counters are always at the end
    if (counterId >= INDEX_COUNTER_RO_INSERT_AVG_TIME)
        return; // cannot reset RO counter

    countersDouble.resetCounter(counterId);
}

void IndexCounters::pegCounter(enum IndexCountersL counterId, long val) noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_L_MAX_ITERATOR)
        return;

    // RO counters are always at the end
    if (counterId >= INDEX_COUNTER_RO_TOTAL_OPERATIONS)
        return; // cannot peg RO counter

    countersLong.pegCounter(counterId, val);
}

void IndexCounters::resetCounter(enum IndexCountersL counterId) noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_L_MAX_ITERATOR)
        return;

    // RO counters are always at the end
    if (counterId >= INDEX_COUNTER_RO_TOTAL_OPERATIONS)
        return; // cannot reset RO counter

    countersLong.resetCounter(counterId);
}

double IndexCounters::getCounterValue(enum IndexCountersD counterId) const noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_D_MAX_ITERATOR)
        return 0.0;

    // rw counter
    if (counterId < INDEX_COUNTER_RO_INSERT_AVG_TIME)
        return countersDouble.getCounterValue(counterId);

    // RO counters need special calculation
    double counterVal = 0.0;
    switch (counterId)
    {
        case INDEX_COUNTER_RO_INSERT_AVG_TIME:
        {
            counterVal = calculateAvg(INDEX_COUNTER_RW_INSERT_TOTAL_TIME, INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS);
            break;
        }
        case INDEX_COUNTER_RO_BULKLOAD_AVG_TIME:
        {
            counterVal = calculateAvg(INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME, INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS);
            break;
        }
        case INDEX_COUNTER_RO_DELETE_AVG_TIME:
        {
            counterVal = calculateAvg(INDEX_COUNTER_RW_DELETE_TOTAL_TIME, INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS);
            break;
        }
        case INDEX_COUNTER_RO_PSEARCH_AVG_TIME:
        {
            counterVal = calculateAvg(INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME, INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS);
            break;
        }
        case INDEX_COUNTER_RO_RSEARCH_AVG_TIME:
        {
            counterVal = calculateAvg(INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME, INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
            break;
        }
        case INDEX_COUNTER_RO_TOTAL_TIME:
        {
            counterVal = countersDouble.getCounterValue(INDEX_COUNTER_RW_INSERT_TOTAL_TIME) +
                         countersDouble.getCounterValue(INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME) +
                         countersDouble.getCounterValue(INDEX_COUNTER_RW_DELETE_TOTAL_TIME) +
                         countersDouble.getCounterValue(INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME) +
                         countersDouble.getCounterValue(INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME);
            break;
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

long IndexCounters::getCounterValue(enum IndexCountersL counterId) const noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_L_MAX_ITERATOR)
        return 0.0;

    // rw counter
    if (counterId < INDEX_COUNTER_RO_TOTAL_OPERATIONS)
        return countersLong.getCounterValue(counterId);

    // RO counters need special calculation
    long counterVal = 0.0;
    switch (counterId)
    {
        case INDEX_COUNTER_RO_TOTAL_OPERATIONS:
        {
            counterVal = countersLong.getCounterValue(INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS);
            break;
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

std::string IndexCounters::getCounterName(enum IndexCountersD counterId) const noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_D_MAX_ITERATOR)
        return std::string("AM_COUNTER");

    return countersDouble.getCounterName(counterId);
}

std::string IndexCounters::getCounterName(enum IndexCountersL counterId) const noexcept(true)
{
    // AM fake counters
    if (counterId >= INDEX_COUNTER_L_MAX_ITERATOR)
        return std::string("AM_COUNTER");

    return countersLong.getCounterName(counterId);
}

std::pair<std::string, double> IndexCounters::getCounter(enum IndexCountersD counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

std::pair<std::string, long> IndexCounters::getCounter(enum IndexCountersL counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

void IndexCounters::resetAllCounters() noexcept(true)
{
    countersLong.resetAllCounters();
    countersDouble.resetAllCounters();
}

std::string IndexCounters::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("IndexCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toString() +
                           std::string(" .countersLong = ") + countersLong.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("IndexCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toString() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toString() +  std::string("\n") +
                           std::string("}"));
}

std::string IndexCounters::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("IndexCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toStringFull() +
                           std::string(" .countersLong = ") + countersLong.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("IndexCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toStringFull() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toStringFull() +  std::string("\n") +
                           std::string("}"));
}
