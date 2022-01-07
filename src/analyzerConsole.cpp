#include <workload/analyzer/analyzerConsole.hpp>
#include <logger/logger.hpp>

#include <iostream>

WorkloadAnalyzerConsole::WorkloadAnalyzerConsole(const std::vector<WorkloadAnalyzerStep*>& steps)
: WorkloadAnalyzer(steps)
{
    LOGGER_LOG_DEBUG("WorkloadAnalyzerConsole created {}");
}

WorkloadAnalyzerConsole::WorkloadAnalyzerConsole(const char*name, const std::vector<WorkloadAnalyzerStep*>& steps)
: WorkloadAnalyzer(name, steps)
{
    LOGGER_LOG_DEBUG("WorkloadAnalyzerConsole created {}");
}

void WorkloadAnalyzerConsole::runAnalyze(const Workload& w)
{
    if (name != nullptr)
        std::cout << name << std::endl;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        const std::string stepString =  steps[i]->analyzeWorkloadCounters(w);
        if (stepString.length() > 0)
            std::cout << steps[i]->getName() << std::endl << stepString << std::endl;
    }
}

WorkloadAnalyzerConsoleDefault::WorkloadAnalyzerConsoleDefault(WorkloadAnalyzerConsoleDefault&& other)
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    name = std::move(other.name);
    steps = std::move(other.steps);
}

WorkloadAnalyzerConsoleDefault& WorkloadAnalyzerConsoleDefault::operator=(WorkloadAnalyzerConsoleDefault&& other)
{
    if (this == &other)
        return *this;

    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    name = std::move(other.name);
    steps = std::move(other.steps);

    return *this;
}

WorkloadAnalyzerConsoleDefaultExtendend::WorkloadAnalyzerConsoleDefaultExtendend(WorkloadAnalyzerConsoleDefaultExtendend&& other)
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    name = std::move(other.name);
    steps = std::move(other.steps);
}

WorkloadAnalyzerConsoleDefaultExtendend& WorkloadAnalyzerConsoleDefaultExtendend::operator=(WorkloadAnalyzerConsoleDefaultExtendend&& other)
{
    if (this == &other)
        return *this;

    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];

    name = std::move(other.name);
    steps = std::move(other.steps);

    return *this;
}