#include <workload/analyzer/analyzerStepQueryTime.hpp>
#include <logger/logger.hpp>

WorkloadAnalyzerStepQueryTime::WorkloadAnalyzerStepQueryTime()
: WorkloadAnalyzerStep("QueryTime")
{

}

std::string WorkloadAnalyzerStepQueryTime::analyzeWorkloadCounters(const Workload& w)
{
    std::string result;
    const std::vector<std::vector<WorkloadCounters>>& stepCounters = w.getAllStepCounters();

    if (stepCounters.size() == 0)
    {
        LOGGER_LOG_WARN("You are trying to analyze workload before execution stepCounters.size() = {}", stepCounters.size());
        return result;
    }

    result += std::string("Type");

    if (w.isInColumnMode() == false)
    {
        const std::vector<DBIndex*>& indexes = w.getAllRawIndexes();
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string("\t") + std::string(indexes[i]->getName());
    }
    else
    {
        const std::vector<DBIndexColumn*>& indexes = w.getAllColumnIndexes();
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string("\t") + std::string(indexes[i]->getName());
    }

    result += std::string("\n");

    for (size_t i = 0; i < stepCounters[0].size(); ++i) // per step
    {
        result += std::to_string(i + 1);
        for (size_t j = 0; j < stepCounters.size(); ++j) // per index
            result += std::string("\t") + std::to_string(stepCounters[j][i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME));


        result += std::string("\n");
    }

    return result;
}