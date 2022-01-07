#ifndef WORKLOAD_ANALYZER_STEP_QUERY_WEAROUT_HPP
#define WORKLOAD_ANALYZER_STEP_QUERY_WEAROUT_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepQueryWearout : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepQueryWearout();


    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepQueryWearout(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepQueryWearout() = default;
    WorkloadAnalyzerStepQueryWearout(const WorkloadAnalyzerStepQueryWearout&) = default;
    WorkloadAnalyzerStepQueryWearout& operator=(const WorkloadAnalyzerStepQueryWearout&) = default;
    WorkloadAnalyzerStepQueryWearout(WorkloadAnalyzerStepQueryWearout &&) = default;
    WorkloadAnalyzerStepQueryWearout& operator=(WorkloadAnalyzerStepQueryWearout &&) = default;
};

#endif