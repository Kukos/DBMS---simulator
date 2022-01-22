#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

#include <numeric>

WorkloadCounters WorkloadStep::collectCounters() noexcept(true)
{
    WorkloadCounters tempCounters;

    if (isColumnIndexMode == false)
    {
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second);

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_AVG_TIME, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_TIME, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_TIME).second);

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT, rIndex->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second + rIndex->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT, rIndex->getDisk().getLowLevelController().getMemoryModel().getMemoryWearOut());

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS, rIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_OPERATIONS).second);
    }
    else
    {
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second);

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_AVG_TIME, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_AVG_TIME).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_TIME, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_TIME).second);

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT, cIndex->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second + cIndex->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT, cIndex->getDisk().getLowLevelController().getMemoryModel().getMemoryWearOut());

        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_INVALIDATION_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_INVALIDATION_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_LOADING_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_LOADING_TOTAL_OPERATIONS).second);
        tempCounters.pegCounter(WorkloadCounters::WORKLOAD_AM_COUNTER_RW_TOTAL_OPERATIONS, cIndex->getCounter(IndexCounters::INDEX_AM_COUNTER_RO_TOTAL_OPERATIONS).second);
    }
    return tempCounters;
}

void WorkloadStep::prepareStep() noexcept(true)
{
    counters = collectCounters();
}

void WorkloadStep::finishStep() noexcept(true)
{
    WorkloadCounters beforeStep = counters;
    WorkloadCounters afterStep = collectCounters();

    counters.resetAllCounters();
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        counters.pegCounter(id, (afterStep.getCounterValue(id) - beforeStep.getCounterValue(id)));

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        counters.pegCounter(id, (afterStep.getCounterValue(id) - beforeStep.getCounterValue(id)));
}

WorkloadStep::WorkloadStep(const char* name, DBIndex* index, DBIndexColumn* columnIndex, size_t numOperations, size_t numEntriesPerOperations, double selectivity, bool isColumnIndexMode, const std::vector<size_t>& col)
:  name{name}, numOperations{numOperations}, numEntriesPerOperations{numEntriesPerOperations}, selectivity{selectivity}, rIndex{index}, cIndex{columnIndex}, isColumnIndexMode{isColumnIndexMode}, columnsToSearch{col}
{
    if (rIndex == nullptr && cIndex == nullptr)
        LOGGER_LOG_DEBUG("WorkloadStep cretaed in config mode {}", toString());
    else
        LOGGER_LOG_DEBUG("WorkloadStep fully created {}", toStringFull());
}

WorkloadStep::WorkloadStep(const char* name, DBIndex* index, size_t numOperations, size_t numEntriesPerOperations, double selectivity)
: WorkloadStep(name, index, nullptr, numOperations, numEntriesPerOperations, selectivity, false, std::vector<size_t>())
{

}

WorkloadStep::WorkloadStep(const char* name, DBIndexColumn* index, size_t numOperations, size_t numEntriesPerOperations, double selectivity)
: WorkloadStep(name, nullptr, index, numOperations, numEntriesPerOperations, selectivity, true,  std::vector<size_t>())
{
    // user do not specyfied which columns want, so use all
    for (size_t i = 0; i < index->getNumOfColumns(); ++i)
        columnsToSearch.push_back(i);
}

WorkloadStep::WorkloadStep(const char* name, DBIndexColumn* index, size_t numOperations, size_t numEntriesPerOperations, double selectivity, const std::vector<size_t>& col)
: WorkloadStep(name, nullptr, index, numOperations, numEntriesPerOperations, selectivity, true,  col)
{

}

WorkloadStep::WorkloadStep(const char* name, size_t numOperations, size_t numEntriesPerOperations, double selectivity)
: WorkloadStep(name, nullptr, nullptr, numOperations, numEntriesPerOperations, selectivity, false, std::vector<size_t>())
{

}

WorkloadStep::WorkloadStep(const char* name, size_t numOperations, size_t numEntriesPerOperations, double selectivity, const std::vector<size_t>& col)
: WorkloadStep(name, nullptr, nullptr, numOperations, numEntriesPerOperations, selectivity, false, col)
{

}


std::string WorkloadStep::toString(bool oneLine) const noexcept(true)
{
    auto buildStringColumns = [](const std::string &accumulator, const size_t &col)
    {
        return accumulator.empty() ? std::to_string(col) : accumulator + "," + std::to_string(col);
    };

    const std::string colString =  std::string("{ ") + std::accumulate(std::begin(columnsToSearch), std::end(columnsToSearch), std::string(), buildStringColumns) + std::string(" }");

    if (oneLine)
        return std::string(std::string("WorkloadStep {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numOperations = ") + std::to_string(numOperations) +
                           std::string(" .numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) +
                           std::string(" .selectivity = ") + std::to_string(selectivity) +
                           std::string(" .isColumnIndexMode = ") + std::to_string(isColumnIndexMode) +
                           std::string(" .columnsToSearch = ") + colString +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadStep {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numOperations = ") + std::to_string(numOperations) + std::string("\n") +
                           std::string("\t.numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) + std::string("\n") +
                           std::string("\t.selectivity = ") + std::to_string(selectivity) + std::string("\n") +
                           std::string("\t.isColumnIndexMode = ") + std::to_string(isColumnIndexMode) + std::string("\n") +
                           std::string("\t.columnsToSearch = ") + colString  +std::string("\n") +
                           std::string("}"));
}

std::string WorkloadStep::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringColumns = [](const std::string &accumulator, const size_t &col)
    {
        return accumulator.empty() ? std::to_string(col) : accumulator + "," + std::to_string(col);
    };

    const std::string colString =  std::string("{ ") + std::accumulate(std::begin(columnsToSearch), std::end(columnsToSearch), std::string(), buildStringColumns) + std::string(" }");

    if (oneLine)
        return std::string(std::string("WorkloadStep {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numOperations = ") + std::to_string(numOperations) +
                           std::string(" .numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) +
                           std::string(" .selectivity = ") + std::to_string(selectivity) +
                           std::string(" .isColumnIndexMode = ") + std::to_string(isColumnIndexMode) +
                           std::string(" .columnsToSearch = ") + colString +
                           std::string(" .rindex = ") + (rIndex == nullptr ? std::string("nullptr") : rIndex->toStringFull()) +
                           std::string(" .cindex = ") + (cIndex == nullptr ? std::string("nullptr") : cIndex->toStringFull()) +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadStep {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numOperations = ") + std::to_string(numOperations) + std::string("\n") +
                           std::string("\t.numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) + std::string("\n") +
                           std::string("\t.selectivity = ") + std::to_string(selectivity) + std::string("\n") +
                           std::string("\t.isColumnIndexMode = ") + std::to_string(isColumnIndexMode) + std::string("\n") +
                           std::string("\t.columnsToSearch = ") + colString  +std::string("\n") +
                           std::string("\t.rIndex = ") + (rIndex == nullptr ? std::string("nullptr") : rIndex->toStringFull()) + std::string("\n") +
                           std::string("\t.cIndex = ") + (cIndex == nullptr ? std::string("nullptr") : cIndex->toStringFull()) + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}