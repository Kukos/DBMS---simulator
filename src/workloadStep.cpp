#include <workload/workloadStep.hpp>
#include <logger/logger.hpp>

WorkloadCounters WorkloadStep::collectCounters() noexcept(true)
{
    WorkloadCounters tempCounters;

    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_AVG_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_INSERT_AVG_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_AVG_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_BULKLOAD_AVG_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_AVG_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_DELETE_AVG_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_AVG_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_PSEARCH_AVG_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_AVG_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_RSEARCH_AVG_TIME).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME, index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_TIME).second);

    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RW_INSERT_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RW_BULKLOAD_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_DELETE_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RW_DELETE_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RW_PSEARCH_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RW_RSEARCH_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_OPERATIONS, index->getCounter(IndexCounters::INDEX_COUNTER_RO_TOTAL_OPERATIONS).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_VIRTUAL_WEAROUT, index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_WRITE_TOTAL_BYTES).second + index->getDisk().getDiskCounter(MemoryCounters::MEMORY_COUNTER_RW_OVERWRITE_TOTAL_BYTES).second);
    tempCounters.pegCounter(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT, index->getDisk().getLowLevelController().getMemoryModel().getMemoryWearOut());

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

WorkloadStep::WorkloadStep(const char* name, DBIndex* index, size_t numOperations, size_t numEntriesPerOperations, double selectivity)
:  name{name}, numOperations{numOperations}, numEntriesPerOperations{numEntriesPerOperations}, selectivity{selectivity}, index{index}
{
    if (index == nullptr)
        LOGGER_LOG_DEBUG("WorkloadStep cretaed in config mode {}", toString());
    else
        LOGGER_LOG_DEBUG("WorkloadStep fully created {}", toStringFull());
}

WorkloadStep::WorkloadStep(const char* name, size_t numOperations, size_t numEntriesPerOperations, double selectivity)
: WorkloadStep(name, nullptr, numOperations, numEntriesPerOperations, selectivity)
{

}

std::string WorkloadStep::toString(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("WorkloadStep {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numOperations = ") + std::to_string(numOperations) +
                           std::string(" .numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) +
                           std::string(" .selectivity = ") + std::to_string(selectivity) +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadStep {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numOperations = ") + std::to_string(numOperations) + std::string("\n") +
                           std::string("\t.numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) + std::string("\n") +
                           std::string("\t.selectivity = ") + std::to_string(selectivity) + std::string("\n") +
                           std::string("}"));
}

std::string WorkloadStep::toStringFull(bool oneLine) const noexcept(true)
{
    if (oneLine)
        return std::string(std::string("WorkloadStep {") +
                           std::string(" .name = ") + std::string(name) +
                           std::string(" .numOperations = ") + std::to_string(numOperations) +
                           std::string(" .numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) +
                           std::string(" .selectivity = ") + std::to_string(selectivity) +
                           std::string(" .index = ") + (index == nullptr ? std::string("nullptr") : index->toStringFull()) +
                           std::string(" .counters = ") + counters.toStringFull() +
                           std::string(" }"));
    else
        return std::string(std::string("WorkloadStep {\n") +
                           std::string("\t.name = ") + std::string(name) + std::string("\n") +
                           std::string("\t.numOperations = ") + std::to_string(numOperations) + std::string("\n") +
                           std::string("\t.numEntriesPerOperations = ") + std::to_string(numEntriesPerOperations) + std::string("\n") +
                           std::string("\t.selectivity = ") + std::to_string(selectivity) + std::string("\n") +
                           std::string("\t.index = ") + (index == nullptr ? std::string("nullptr") : index->toStringFull()) + std::string("\n") +
                           std::string("\t.counters = ") + counters.toStringFull() + std::string("\n") +
                           std::string("}"));
}