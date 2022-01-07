#include <workload/analyzer/analyzerFile.hpp>
#include <logger/logger.hpp>

#include <fstream>

WorkloadAnalyzerFile::WorkloadAnalyzerFile(const char* filePath, const std::vector<WorkloadAnalyzerStep*>& steps)
: WorkloadAnalyzer(steps), filePath{filePath}
{
    LOGGER_LOG_DEBUG("WorkloadAnalyzerFile created {}");
}

void WorkloadAnalyzerFile::runAnalyze(const Workload& w)
{
    if (filePath == nullptr || filePath == NULL)
        return;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        const std::string fileName = std::string(filePath) + std::string(steps[i]->getName()) + std::string(".txt");
        std::ofstream file(fileName);
        file << steps[i]->analyzeWorkloadCounters(w);
        file.close();
    }
}

WorkloadAnalyzerFileDefault::WorkloadAnalyzerFileDefault(WorkloadAnalyzerFileDefault&& other)
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    filePath = std::move(other.filePath);
    steps = std::move(other.steps);
}

WorkloadAnalyzerFileDefault& WorkloadAnalyzerFileDefault::operator=(WorkloadAnalyzerFileDefault&& other)
{
    if (this == &other)
        return *this;

    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    filePath = std::move(other.filePath);
    steps = std::move(other.steps);

    return *this;
}

WorkloadAnalyzerFileDefaultExtendend::WorkloadAnalyzerFileDefaultExtendend(WorkloadAnalyzerFileDefaultExtendend&& other)
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    filePath = std::move(other.filePath);
    steps = std::move(other.steps);
}

WorkloadAnalyzerFileDefaultExtendend& WorkloadAnalyzerFileDefaultExtendend::operator=(WorkloadAnalyzerFileDefaultExtendend&& other)
{
    if (this == &other)
        return *this;

    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    filePath = std::move(other.filePath);
    steps = std::move(other.steps);

    return *this;
}