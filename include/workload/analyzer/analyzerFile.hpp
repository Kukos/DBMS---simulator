#ifndef WORKLOAD_ANALYZER_FILE_HPP
#define WORKLOAD_ANALYZER_FILE_HPP

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

class WorkloadAnalyzerFile : public WorkloadAnalyzer
{
protected:
    const char* filePath;
public:

    /**
     * @brief Construct a new Workload Analyzer object
     *
     * @param[in] steps - vector with steps
     */
    WorkloadAnalyzerFile(const char* filePath, const std::vector<WorkloadAnalyzerStep*>& steps);

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
        return new WorkloadAnalyzerFile(*this);
    }

    virtual ~WorkloadAnalyzerFile() = default;
    WorkloadAnalyzerFile(const WorkloadAnalyzerFile&) = default;
    WorkloadAnalyzerFile& operator=(const WorkloadAnalyzerFile&) = default;
    WorkloadAnalyzerFile() = default;
    WorkloadAnalyzerFile(WorkloadAnalyzerFile &&) = default;
    WorkloadAnalyzerFile& operator=(WorkloadAnalyzerFile &&) = default;

};

class WorkloadAnalyzerFileDefault : public WorkloadAnalyzerFile
{
public:
    WorkloadAnalyzerFileDefault(const char* filePath)
    {
        this->filePath = filePath;
        steps.push_back(new WorkloadAnalyzerStepTotalTime());
        steps.push_back(new WorkloadAnalyzerStepTotalTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepTotalWearout());
        steps.push_back(new WorkloadAnalyzerStepTotalWearoutNormalized());
    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerFileDefault(*this);
    }

    virtual ~WorkloadAnalyzerFileDefault() = default;
    WorkloadAnalyzerFileDefault() = default;
    WorkloadAnalyzerFileDefault(const WorkloadAnalyzerFileDefault&) = default;
    WorkloadAnalyzerFileDefault& operator=(const WorkloadAnalyzerFileDefault&) = default;
    WorkloadAnalyzerFileDefault(WorkloadAnalyzerFileDefault &&);
    WorkloadAnalyzerFileDefault& operator=(WorkloadAnalyzerFileDefault &&);
};

class WorkloadAnalyzerFileDefaultExtendend : public WorkloadAnalyzerFile
{
public:
    WorkloadAnalyzerFileDefaultExtendend(const char* filePath)
    {
        this->filePath = filePath;
        steps.push_back(new WorkloadAnalyzerStepTotalTime());
        steps.push_back(new WorkloadAnalyzerStepTotalTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepTotalWearout());
        steps.push_back(new WorkloadAnalyzerStepTotalWearoutNormalized());

        steps.push_back(new WorkloadAnalyzerStepQueryTime());
        steps.push_back(new WorkloadAnalyzerStepQueryTimeNormalized());
        steps.push_back(new WorkloadAnalyzerStepQueryWearout());
        steps.push_back(new WorkloadAnalyzerStepQueryWearoutNormalized());
    }

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new WorkloadAnalyzer
    *
    * @return new WorkloadAnalyzer
    */
    virtual WorkloadAnalyzer* clone() const noexcept(true) override
    {
        return new WorkloadAnalyzerFileDefaultExtendend(*this);
    }

    virtual ~WorkloadAnalyzerFileDefaultExtendend() = default;
    WorkloadAnalyzerFileDefaultExtendend() = default;
    WorkloadAnalyzerFileDefaultExtendend(const WorkloadAnalyzerFileDefaultExtendend&) = default;
    WorkloadAnalyzerFileDefaultExtendend& operator=(const WorkloadAnalyzerFileDefaultExtendend&) = default;
    WorkloadAnalyzerFileDefaultExtendend(WorkloadAnalyzerFileDefaultExtendend &&);
    WorkloadAnalyzerFileDefaultExtendend& operator=(WorkloadAnalyzerFileDefaultExtendend &&);
};

#endif