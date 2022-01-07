#ifndef WORKLOAD_ANALYZER_STEP_QUERY_TIME_HPP
#define WORKLOAD_ANALYZER_STEP_QUERY_TIME_HPP

#include <workload/analyzer/analyzerStep.hpp>

class WorkloadAnalyzerStepQueryTime : public WorkloadAnalyzerStep
{
public:

    WorkloadAnalyzerStepQueryTime();

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerStepQueryTime(*this);
    }

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) override;

    virtual ~WorkloadAnalyzerStepQueryTime() = default;
    WorkloadAnalyzerStepQueryTime(const WorkloadAnalyzerStepQueryTime&) = default;
    WorkloadAnalyzerStepQueryTime& operator=(const WorkloadAnalyzerStepQueryTime&) = default;
    WorkloadAnalyzerStepQueryTime(WorkloadAnalyzerStepQueryTime &&) = default;
    WorkloadAnalyzerStepQueryTime& operator=(WorkloadAnalyzerStepQueryTime &&) = default;
};

#endif