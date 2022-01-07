#ifndef WORKLOAD_ANALYZER_STEP_QUERY_TIME_NORMALIZED_HPP
#define WORKLOAD_ANALYZER_STEP_QUERY_TIME_NORMALIZED_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepQueryTimeNormalized : public WorkloadAnalyzerStep
{
public:
    WorkloadAnalyzerStepQueryTimeNormalized();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepQueryTimeNormalized(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepQueryTimeNormalized() = default;
    WorkloadAnalyzerStepQueryTimeNormalized(const WorkloadAnalyzerStepQueryTimeNormalized&) = default;
    WorkloadAnalyzerStepQueryTimeNormalized& operator=(const WorkloadAnalyzerStepQueryTimeNormalized&) = default;
    WorkloadAnalyzerStepQueryTimeNormalized(WorkloadAnalyzerStepQueryTimeNormalized &&) = default;
    WorkloadAnalyzerStepQueryTimeNormalized& operator=(WorkloadAnalyzerStepQueryTimeNormalized &&) = default;
};

#endif