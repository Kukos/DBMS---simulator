#include <workload/analyzer/analyzerStepQueryWearoutNormalized.hpp>
#include <logger/logger.hpp>

WorkloadAnalyzerStepQueryWearoutNormalized::WorkloadAnalyzerStepQueryWearoutNormalized()
: WorkloadAnalyzerStep("QueryWearoutNormalized")
{

}

std::string WorkloadAnalyzerStepQueryWearoutNormalized::analyzeWorkloadCounters(const Workload& w)
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
        const size_t firstWearout = stepCounters[0][i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT);
        if (firstWearout == 0)
            LOGGER_LOG_WARN("You wanted to normalized to {} value which is 0", firstWearout);

        result += std::to_string(i + 1);
        for (size_t j = 0; j < stepCounters.size(); ++j) // per index
            result += std::string("\t") + std::to_string(firstWearout == 0 ? 0.0 : static_cast<double>(stepCounters[j][i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / static_cast<double>(firstWearout));


        result += std::string("\n");
    }

    return result;
}