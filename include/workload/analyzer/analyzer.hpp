#ifndef WORKLOAD_ANALYZER_HPP
#define WORKLOAD_ANALYZER_HPP

#include <workload/workload.hpp>
#include <workload/analyzer/analyzerStep.hpp>

#include <vector>

class WorkloadAnalyzer
{
protected:
    const char* name;
    std::vector<WorkloadAnalyzerStep*> steps;
public:
    /**
     * @brief Construct a new Workload Analyzer object
     *
     * @param[in] steps - vector with steps
     */
    WorkloadAnalyzer(const std::vector<WorkloadAnalyzerStep*>& steps);

    /**
     * @brief Construct a new Workload Analyzer object
     *
     * @param[in] name - Alanylzer name (label)
     * @param[in] steps - vector with steps
     */
    WorkloadAnalyzer(const char*name, const std::vector<WorkloadAnalyzerStep*>& steps);

    /**
     * @brief  Add new step
     *
     * @param[in] step - new step to add
     */
    void addStep(WorkloadAnalyzerStep* step);

    /**
     * @brief Run all analyzers
     *
     * @param[in] w - workload to analyze
     */
    virtual void runAnalyze(const Workload& w) = 0;

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) = 0;

    virtual ~WorkloadAnalyzer();
    WorkloadAnalyzer(const WorkloadAnalyzer&);
    WorkloadAnalyzer& operator=(const WorkloadAnalyzer&);

    WorkloadAnalyzer() = default;
    WorkloadAnalyzer(WorkloadAnalyzer &&) = default;
    WorkloadAnalyzer& operator=(WorkloadAnalyzer &&) = default;
};

#endif