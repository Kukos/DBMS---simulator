#include <workload/analyzer/analyzerStepTotalWearout.hpp>
#include <logger/logger.hpp>

WorkloadAnalyzerStepTotalWearout::WorkloadAnalyzerStepTotalWearout()
: WorkloadAnalyzerStep("TotalWearout")
{

}

std::string WorkloadAnalyzerStepTotalWearout::analyzeWorkloadCounters(const Workload& w)
{
    std::string result;
    const std::vector<WorkloadCounters>& totalCounters = w.getAllTotalCounters();

    if (totalCounters.size() == 0)
    {
        LOGGER_LOG_WARN("You are trying to analyze workload before execution totalCounters.size() = {}", totalCounters.size());
        return result;
    }

    if (w.isInColumnMode() == false)
    {
        const std::vector<DBIndex*>& indexes = w.getAllRawIndexes();
        result += std::string("Type\tWearout\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) + std::string("\n");
    }
    else
    {
        const std::vector<DBIndexColumn*>& indexes = w.getAllColumnIndexes();
        result += std::string("Type\tWearout\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_PHYSICAL_WEAROUT)) + std::string("\n");
    }

    return result;
}