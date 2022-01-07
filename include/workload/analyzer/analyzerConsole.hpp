#ifndef WORKLOAD_ANALYZER_CONSOLE_HPP
#define WORKLOAD_ANALYZER_CONSOLE_HPP

#include <workload/analyzer/analyzer.hpp>
#include <workload/analyzer/analyzerStep.hpp>
#include <workload/analyzer/analyzerStepTotalTime.hpp>
#include <workload/analyzer/analyzerStepTotalTimeNormalized.hpp>
#include <workload/analyzer/analyzerStepTotalWearout.hpp>
#include <workload/analyzer/analyzerStepTotalWearoutNormalized.hpp>
#include <workload/analyzer/analyzerStepQueryTime.hpp>
#include <workload/analyzer/analyzerStepQueryTimeNormalized.hpp>
#include <workload/analyzer/analyzerStepQueryWearout.hpp>
#include <workload/analyzer/analyzerStepQueryWearoutNormalized.hpp>

class WorkloadAnalyzerConsole : public WorkloadAnalyzer
{
public:

    /**
     * @brief Construct a new Workload Analyzer object
     *
     * @param[in] steps - vector with steps
     */
    WorkloadAnalyzerConsole(const std::vector<WorkloadAnalyzerStep*>& steps);

    /**
     * @brief Construct a new Workload Analyzer object
     *
     * @param[in] steps - vector with steps
     */
    WorkloadAnalyzerConsole(const char*name, const std::vector<WorkloadAnalyzerStep*>& steps);

    /**
     * @brief Run all analyzers
     *
     * @param[in] w - workload to analyze
     */
    virtual void runAnalyze(const Workload& w) override;

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerConsole(*this);
    }

    virtual ~WorkloadAnalyzerConsole() = default;
    WorkloadAnalyzerConsole(const WorkloadAnalyzerConsole&) = default;
    WorkloadAnalyzerConsole& operator=(const WorkloadAnalyzerConsole&) = default;
    WorkloadAnalyzerConsole() = default;
    WorkloadAnalyzerConsole(WorkloadAnalyzerConsole &&) = default;
    WorkloadAnalyzerConsole& operator=(WorkloadAnalyzerConsole &&) = default;

};

class WorkloadAnalyzerConsoleDefault : public WorkloadAnalyzerConsole
{
public:
    WorkloadAnalyzerConsoleDefault(const char* name)
    {
        this->name = name;
        steps.push_back(new WorkloadAnalyzerStepTotalTime());
        steps.push_back(new WorkloadAnalyzerStepTotalTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepTotalWearout());
        steps.push_back(new WorkloadAnalyzerStepTotalWearoutNormalized());
    }

    WorkloadAnalyzerConsoleDefault()
    : WorkloadAnalyzerConsoleDefault(nullptr)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerConsoleDefault(*this);
    }

    virtual ~WorkloadAnalyzerConsoleDefault() = default;
    WorkloadAnalyzerConsoleDefault(const WorkloadAnalyzerConsoleDefault&) = default;
    WorkloadAnalyzerConsoleDefault& operator=(const WorkloadAnalyzerConsoleDefault&) = default;

    WorkloadAnalyzerConsoleDefault(WorkloadAnalyzerConsoleDefault &&);
    WorkloadAnalyzerConsoleDefault& operator=(WorkloadAnalyzerConsoleDefault &&);
};

class WorkloadAnalyzerConsoleDefaultExtendend : public WorkloadAnalyzerConsole
{
public:
    WorkloadAnalyzerConsoleDefaultExtendend(const char* name)
    {
        this->name = name;
        steps.push_back(new WorkloadAnalyzerStepTotalTime());
        steps.push_back(new WorkloadAnalyzerStepTotalTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepTotalWearout());
        steps.push_back(new WorkloadAnalyzerStepTotalWearoutNormalized());

        steps.push_back(new WorkloadAnalyzerStepQueryTime());
        steps.push_back(new WorkloadAnalyzerStepQueryTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepQueryWearout());
        steps.push_back(new WorkloadAnalyzerStepQueryWearoutNormalized());
    }

    WorkloadAnalyzerConsoleDefaultExtendend()
    : WorkloadAnalyzerConsoleDefaultExtendend(nullptr)
    {

    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerConsoleDefaultExtendend(*this);
    }

    virtual ~WorkloadAnalyzerConsoleDefaultExtendend() = default;
    WorkloadAnalyzerConsoleDefaultExtendend(const WorkloadAnalyzerConsoleDefaultExtendend&) = default;
    WorkloadAnalyzerConsoleDefaultExtendend& operator=(const WorkloadAnalyzerConsoleDefaultExtendend&) = default;

    WorkloadAnalyzerConsoleDefaultExtendend(WorkloadAnalyzerConsoleDefaultExtendend &&);
    WorkloadAnalyzerConsoleDefaultExtendend& operator=(WorkloadAnalyzerConsoleDefaultExtendend &&);
};

#endif