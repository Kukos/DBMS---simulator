#include <workload/analyzer/analyzerStepTotalWearoutNormalized.hpp>
#include <logger/logger.hpp>

WorkloadAnalyzerStepTotalWearoutNormalized::WorkloadAnalyzerStepTotalWearoutNormalized()
: WorkloadAnalyzerStep("TotalWearoutNormalized")
{

}

std::string WorkloadAnalyzerStepTotalWearoutNormalized::analyzeWorkloadCounters(const Workload& w)
{
    std::string result;
    const std::vector<WorkloadCounters>& totalCounters = w.getAllTotalCounters();

    if (totalCounters.size() == 0)
    {
        LOGGER_LOG_WARN("You are trying to analyze workload before execution totalCounters.size() = {}", totalCounters.size());
        return result;
    }

    const size_t firstWearout = totalCounters[0].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT);
    if (firstWearout == 0)
        LOGGER_LOG_WARN("You wanted to normalized to {} value which is 0", firstWearout);

    if (w.isInColumnMode() == false)
    {
        const std::vector<DBIndex*>& indexes = w.getAllRawIndexes();
        result += std::string("Type\tWearout\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(firstWearout == 0 ? 0.0 : static_cast<double>(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / static_cast<double>(firstWearout)) + std::string("\n");
    }
    else
    {
        const std::vector<DBIndexColumn*>& indexes = w.getAllColumnIndexes();
        result += std::string("Type\tWearout\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(firstWearout == 0 ? 0.0 : static_cast<double>(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) / static_cast<double>(firstWearout)) + std::string("\n");
    }

    return result;
}