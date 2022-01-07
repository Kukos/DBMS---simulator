#ifndef WORKLOAD_ANALYZER_STEP_TOTAL_TIME_NORMALIZED_HPP
#define WORKLOAD_ANALYZER_STEP_TOTAL_TIME_NORMALIZED_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepTotalTimeNormalized : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepTotalTimeNormalized();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepTotalTimeNormalized(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepTotalTimeNormalized() = default;
    WorkloadAnalyzerStepTotalTimeNormalized(const WorkloadAnalyzerStepTotalTimeNormalized&) = default;
    WorkloadAnalyzerStepTotalTimeNormalized& operator=(const WorkloadAnalyzerStepTotalTimeNormalized&) = default;
    WorkloadAnalyzerStepTotalTimeNormalized(WorkloadAnalyzerStepTotalTimeNormalized &&) = default;
    WorkloadAnalyzerStepTotalTimeNormalized& operator=(WorkloadAnalyzerStepTotalTimeNormalized &&) = default;
};

#endif