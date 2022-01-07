#ifndef WORKLOAD_ANALYZER_STEP_HPP
#define WORKLOAD_ANALYZER_STEP_HPP

#include <workload/workload.hpp>

class WorkloadAnalyzerStep
{
protected:
    const char* name;
public:
    /**
     * @brief Construct a new Workload Analyzer Step object
     *
     * @param[in] name - analyzer name
     */
    WorkloadAnalyzerStep(const char* name)
    : name{name}
    {

    }

    /**
     * @brief Get the Name
     *
     * @return name
     */
    const char* getName() const noexcept(true)
    {
        return name;
    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzerStep
    *
    * @return new WorkloadAnalyzerStep
    */
    virtual WorkloadAnalyzerStep* clone() const noexcept(true) = 0;

    /**
     * @brief Create string from analysis like total Time string for all indexes
     *
     * @param[in] w - workload to analyze
     * @return std::string
     */
    virtual std::string analyzeWorkloadCounters(const Workload& w) = 0;

    virtual ~WorkloadAnalyzerStep() = default;
    WorkloadAnalyzerStep() = default;
    WorkloadAnalyzerStep(const WorkloadAnalyzerStep&) = default;
    WorkloadAnalyzerStep& operator=(const WorkloadAnalyzerStep&) = default;
    WorkloadAnalyzerStep(WorkloadAnalyzerStep &&) = default;
    WorkloadAnalyzerStep& operator=(WorkloadAnalyzerStep &&) = default;
};

#endif