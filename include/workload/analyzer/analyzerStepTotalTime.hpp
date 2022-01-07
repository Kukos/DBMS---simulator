#ifndef WORKLOAD_ANALYZER_STEP_TOTAL_TIME_HPP
#define WORKLOAD_ANALYZER_STEP_TOTAL_TIME_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepTotalTime : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepTotalTime();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepTotalTime(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepTotalTime() = default;
    WorkloadAnalyzerStepTotalTime(const WorkloadAnalyzerStepTotalTime&) = default;
    WorkloadAnalyzerStepTotalTime& operator=(const WorkloadAnalyzerStepTotalTime&) = default;
    WorkloadAnalyzerStepTotalTime(WorkloadAnalyzerStepTotalTime &&) = default;
    WorkloadAnalyzerStepTotalTime& operator=(WorkloadAnalyzerStepTotalTime &&) = default;
};

#endif