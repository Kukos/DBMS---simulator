#include <observability/memoryCounters.hpp>
#include <logger/logger.hpp>

#define TO_STRING_PRIV(X) #X
#define TO_STRING(X) TO_STRING_PRIV(X)

double MemoryCounters::calculateAvg(enum MemoryCountersD total, enum MemoryCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const double totalCost = countersDouble.getCounterValue(total);

    if (numOfOperation == 0)
        return  0.0;

    return totalCost / static_cast<double>(numOfOperation);
}

double MemoryCounters::calculateAvg(enum MemoryCountersL total, enum MemoryCountersL numberOfOp) const noexcept(true)
{
    const long numOfOperation = countersLong.getCounterValue(numberOfOp);
    const long totalCost = countersLong.getCounterValue(total);

    if (numOfOperation == 0)
        return 0.0;

    return  static_cast<double>(totalCost) / static_cast<double>(numOfOperation);
}

MemoryCounters::MemoryCounters()
{
    // add double counters
    countersDouble.addCounter(MEMORY_COUNTER_RW_READ_TOTAL_TIME, std::string(TO_STRING(MEMORY_COUNTER_RW_READ_TOTAL_TIME)));
    countersDouble.addCounter(MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, std::string(TO_STRING(MEMORY_COUNTER_RW_WRITE_TOTAL_TIME)));
    countersDouble.addCounter(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, std::string(TO_STRING(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME)));

    // add RO counters to get name in easy way
    countersDouble.addCounter(MEMORY_COUNTER_RO_READ_AVG_TIME, std::string(TO_STRING(MEMORY_COUNTER_RO_READ_AVG_TIME)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_WRITE_AVG_TIME, std::string(TO_STRING(MEMORY_COUNTER_RO_WRITE_AVG_TIME)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME, std::string(TO_STRING(MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_READ_AVG_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RO_READ_AVG_BYTES)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_WRITE_AVG_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RO_WRITE_AVG_BYTES)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES)));
    countersDouble.addCounter(MEMORY_COUNTER_RO_TOTAL_TIME, std::string(TO_STRING(MEMORY_COUNTER_RO_TOTAL_TIME)));


    // add long counters
    countersLong.addCounter(MEMORY_COUNTER_RW_READ_TOTAL_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RW_READ_TOTAL_BYTES)));
    countersLong.addCounter(MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES)));
    countersLong.addCounter(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES)));
    countersLong.addCounter(MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS, std::string(TO_STRING(MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS)));
    countersLong.addCounter(MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS, std::string(TO_STRING(MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS)));
    countersLong.addCounter(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS, std::string(TO_STRING(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS)));

    // add RO counters to get name is easy way
    countersLong.addCounter(MEMORY_COUNTER_RO_TOTAL_BYTES, std::string(TO_STRING(MEMORY_COUNTER_RO_TOTAL_BYTES)));
    countersLong.addCounter(MEMORY_COUNTER_RO_TOTAL_OPERATIONS, std::string(TO_STRING(MEMORY_COUNTER_RO_TOTAL_OPERATIONS)));

    LOGGER_LOG_DEBUG("Memory counters created {}", toStringFull());
}

void MemoryCounters::pegCounter(enum MemoryCountersD counterId, double val) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= MEMORY_COUNTER_RO_READ_AVG_TIME)
        return; // cannot peg RO counter

    countersDouble.pegCounter(counterId, val);
}

void MemoryCounters::resetCounter(enum MemoryCountersD counterId) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= MEMORY_COUNTER_RO_READ_AVG_TIME)
        return; // cannot reset RO counter

    countersDouble.resetCounter(counterId);
}

void MemoryCounters::pegCounter(enum MemoryCountersL counterId, long val) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= MEMORY_COUNTER_RO_TOTAL_BYTES)
        return; // cannot peg RO counter

    countersLong.pegCounter(counterId, val);
}

void MemoryCounters::resetCounter(enum MemoryCountersL counterId) noexcept(true)
{
    // RO counters are always at the end
    if (counterId >= MEMORY_COUNTER_RO_TOTAL_BYTES)
        return; // cannot reset RO counter

    countersLong.resetCounter(counterId);
}

double MemoryCounters::getCounterValue(enum MemoryCountersD counterId) const noexcept(true)
{
    // rw counter
    if (counterId < MEMORY_COUNTER_RO_READ_AVG_TIME)
        return countersDouble.getCounterValue(counterId);

    // RO counters need special calculation
    double counterVal = 0.0;
    switch (counterId)
    {
        case MEMORY_COUNTER_RO_READ_AVG_TIME:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_READ_TOTAL_TIME, MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_WRITE_AVG_TIME:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_WRITE_TOTAL_TIME, MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_OVERWRITE_AVG_TIME:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME, MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_READ_AVG_BYTES:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_READ_TOTAL_BYTES, MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_WRITE_AVG_BYTES:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES, MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_OVERWRITE_AVG_BYTES:
        {
            counterVal = calculateAvg(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES, MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS);
            break;
        }
        case MEMORY_COUNTER_RO_TOTAL_TIME:
        {
            counterVal = countersDouble.getCounterValue(MEMORY_COUNTER_RW_READ_TOTAL_TIME) +
                         countersDouble.getCounterValue(MEMORY_COUNTER_RW_WRITE_TOTAL_TIME) +
                         countersDouble.getCounterValue(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_TIME);
            break;
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

long MemoryCounters::getCounterValue(enum MemoryCountersL counterId) const noexcept(true)
{
    // rw counter
    if (counterId < MEMORY_COUNTER_RO_TOTAL_BYTES)
        return countersLong.getCounterValue(counterId);

    // RO counters need special calculation
    long counterVal = 0.0;
    switch (counterId)
    {
        case MEMORY_COUNTER_RO_TOTAL_BYTES:
        {
            counterVal = countersLong.getCounterValue(MEMORY_COUNTER_RW_READ_TOTAL_BYTES) +
                         countersLong.getCounterValue(MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES) +
                         countersLong.getCounterValue(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES);
            break;
        }
        case MEMORY_COUNTER_RO_TOTAL_OPERATIONS:
        {
            counterVal = countersLong.getCounterValue(MEMORY_COUNTER_RW_READ_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(MEMORY_COUNTER_RW_WRITE_TOTAL_OPERATIONS) +
                         countersLong.getCounterValue(MEMORY_COUNTER_RW_OVERWRITE_TOTAL_OPERATIONS);
        }
        default:
        {
            break;
        }
    }

    return counterVal;
}

std::string MemoryCounters::getCounterName(enum MemoryCountersD counterId) const noexcept(true)
{
    return countersDouble.getCounterName(counterId);
}

std::string MemoryCounters::getCounterName(enum MemoryCountersL counterId) const noexcept(true)
{
    return countersLong.getCounterName(counterId);
}

std::pair<std::string, double> MemoryCounters::getCounter(enum MemoryCountersD counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

std::pair<std::string, long> MemoryCounters::getCounter(enum MemoryCountersL counterId) const noexcept(true)
{
    return std::make_pair(getCounterName(counterId), getCounterValue(counterId));
}

void MemoryCounters::resetAllCounters() noexcept(true)
{
    countersLong.resetAllCounters();
    countersDouble.resetAllCounters();
}

std::string MemoryCounters::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toString() +
                           std::string(" .countersLong = ") + countersLong.toString() +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toString() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toString() +  std::string("\n") +
                           std::string("}"));
}

std::string MemoryCounters::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("MemoryCounters = {") +
                           std::string(" .countersDouble = ") + countersDouble.toStringFull() +
                           std::string(" .countersLong = ") + countersLong.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("MemoryCounters = {\n") +
                           std::string("\t .countersDouble = ") + countersDouble.toStringFull() +  std::string("\n") +
                           std::string("\t .countersLong = ") + countersLong.toStringFull() +  std::string("\n") +
                           std::string("}"));
}
