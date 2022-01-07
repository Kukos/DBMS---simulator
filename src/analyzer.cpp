#include <workload/analyzer/analyzer.hpp>
#include <logger/logger.hpp>

WorkloadAnalyzer::WorkloadAnalyzer(const std::vector<WorkloadAnalyzerStep*>& steps)
: name{nullptr}, steps{steps}
{
    LOGGER_LOG_DEBUG("WorkloadAnalyzer created");
}

WorkloadAnalyzer::WorkloadAnalyzer(const char*name, const std::vector<WorkloadAnalyzerStep*>& steps)
: name{name}, steps{steps}
{
    LOGGER_LOG_DEBUG("WorkloadAnalyzer created");
}

WorkloadAnalyzer::~WorkloadAnalyzer()
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];
}

WorkloadAnalyzer::WorkloadAnalyzer(const WorkloadAnalyzer& other)
{
    name = other.name;
    for (size_t i = 0; i < other.steps.size(); ++i)
        steps.push_back(other.steps[i]->clone());
}

WorkloadAnalyzer& WorkloadAnalyzer::operator=(const WorkloadAnalyzer& other)
{
    if (this == &other)
        return *this;

    name = other.name;
    for (size_t i = 0; i < other.steps.size(); ++i)
        steps.push_back(other.steps[i]->clone());

    return *this;
}

void WorkloadAnalyzer::addStep(WorkloadAnalyzerStep* step)
{
    steps.push_back(step);
}

