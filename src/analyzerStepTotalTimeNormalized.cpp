#include <workload/analyzer/analyzerStepTotalTimeNormalized.hpp>
#include <logger/logger.hpp>

#include <cmath>

WorkloadAnalyzerStepTotalTimeNormalized::WorkloadAnalyzerStepTotalTimeNormalized()
: WorkloadAnalyzerStep("TotalTimeNormalized")
{

}


std::string WorkloadAnalyzerStepTotalTimeNormalized::analyzeWorkloadCounters(const Workload& w)
{
    std::string result;
    const std::vector<WorkloadCounters>& totalCounters = w.getAllTotalCounters();

    if (totalCounters.size() == 0)
    {
        LOGGER_LOG_WARN("You are trying to analyze workload before execution totalCounters.size() = {}", totalCounters.size());
        return result;
    }

    const double firstTime = totalCounters[0].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME);
    if (std::fabs(firstTime) <= std::numeric_limits<double>::epsilon())
    {
        LOGGER_LOG_ERROR("You wanted to normalized to {} value which is 0", firstTime);
        return result;
    }

    if (w.isInColumnMode() == false)
    {
        const std::vector<DBIndex*>& indexes = w.getAllRawIndexes();
        result += std::string("Type\tTime\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME) / firstTime) + std::string("\n");
    }
    else
    {
        const std::vector<DBIndexColumn*>& indexes = w.getAllColumnIndexes();
        result += std::string("Type\tTime\n");
        for (size_t i = 0; i < indexes.size(); ++i)
            result += std::string(indexes[i]->getName()) + std::string("\t") + std::to_string(totalCounters[i].getCounterValue(WorkloadCounters::WORKLOAD_COUNTER_RW_TOTAL_TIME) / firstTime) + std::string("\n");
    }

    return result;
}