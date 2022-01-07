#ifndef WORKLOAD_ANALYZER_STEP_TOTAL_WEAROUT_HPP
#define WORKLOAD_ANALYZER_STEP_TOTAL_WEAROUT_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepTotalWearout : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepTotalWearout();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepTotalWearout(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepTotalWearout() = default;
    WorkloadAnalyzerStepTotalWearout(const WorkloadAnalyzerStepTotalWearout&) = default;
    WorkloadAnalyzerStepTotalWearout& operator=(const WorkloadAnalyzerStepTotalWearout&) = default;
    WorkloadAnalyzerStepTotalWearout(WorkloadAnalyzerStepTotalWearout &&) = default;
    WorkloadAnalyzerStepTotalWearout& operator=(WorkloadAnalyzerStepTotalWearout &&) = default;
};

#endif