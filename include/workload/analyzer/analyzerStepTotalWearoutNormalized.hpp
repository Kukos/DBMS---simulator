#ifndef WORKLOAD_ANALYZER_STEP_TOTAL_WEAROUT_NORMALIZED_HPP
#define WORKLOAD_ANALYZER_STEP_TOTAL_WEAROUT_NORMALIZED_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepTotalWearoutNormalized : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepTotalWearoutNormalized();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepTotalWearoutNormalized(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepTotalWearoutNormalized() = default;
    WorkloadAnalyzerStepTotalWearoutNormalized(const WorkloadAnalyzerStepTotalWearoutNormalized&) = default;
    WorkloadAnalyzerStepTotalWearoutNormalized& operator=(const WorkloadAnalyzerStepTotalWearoutNormalized&) = default;
    WorkloadAnalyzerStepTotalWearoutNormalized(WorkloadAnalyzerStepTotalWearoutNormalized &&) = default;
    WorkloadAnalyzerStepTotalWearoutNormalized& operator=(WorkloadAnalyzerStepTotalWearoutNormalized &&) = default;
};

#endif