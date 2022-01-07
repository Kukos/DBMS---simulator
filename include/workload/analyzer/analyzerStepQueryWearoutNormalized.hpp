#ifndef WORKLOAD_ANALYZER_STEP_QUERY_WEAROUT_NORMALIZED_HPP
#define WORKLOAD_ANALYZER_STEP_QUERY_WEAROUT_NORMALIZED_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepQueryWearoutNormalized : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepQueryWearoutNormalized();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepQueryWearoutNormalized(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepQueryWearoutNormalized() = default;
    WorkloadAnalyzerStepQueryWearoutNormalized(const WorkloadAnalyzerStepQueryWearoutNormalized&) = default;
    WorkloadAnalyzerStepQueryWearoutNormalized& operator=(const WorkloadAnalyzerStepQueryWearoutNormalized&) = default;
    WorkloadAnalyzerStepQueryWearoutNormalized(WorkloadAnalyzerStepQueryWearoutNormalized &&) = default;
    WorkloadAnalyzerStepQueryWearoutNormalized& operator=(WorkloadAnalyzerStepQueryWearoutNormalized &&) = default;
};

#endif