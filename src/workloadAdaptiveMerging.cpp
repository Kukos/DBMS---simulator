#include <workload/workloadAdaptiveMerging.hpp>
#include <logger/logger.hpp>
#include <adaptiveMerging/adaptiveMergingFramework.hpp>
#include <workload/workloadStep.hpp>
#include <workload/workloadStepRSearch.hpp>

#include <numeric>

WorkloadAdaptiveMerging::WorkloadAdaptiveMerging(const std::vector<DBIndex*>& indexes, double sel)
: Workload(indexes, std::vector<WorkloadStep*>()), sel{sel}
{
    LOGGER_LOG_DEBUG("WorkloadAdaptiveMerging created {}", toStringFull());
}

void WorkloadAdaptiveMerging::run() noexcept(true)
{
    const size_t indexesSize = rIndexes.size();

    for (size_t i = 0; i < indexesSize; ++i)
    {
        WorkloadCounters totalStats;
        std::vector<WorkloadCounters> stepStats;

        // run steps for index(i)
        while (dynamic_cast<AdaptiveMergingFramework*>(rIndexes[i])->getMemoryManager().getNumEntries() > 0)
        {
            WorkloadStep* step = new WorkloadStepRSearch(rIndexes[i], sel);
            if (i == 0)
                addStep(step);

            step->executeStep();

            const WorkloadCounters& stats = step->getCounters();
            stepStats.push_back(stats);
            aggregateCounters(totalStats, stats);

            if (i != 0)
                delete step;
        }

        totalCounters.push_back(totalStats);
        stepCounters.push_back(stepStats);
    }
}


std::string WorkloadAdaptiveMerging::toString(bool oneLine) const noexcept(true)
{
    auto buildStringFromSteps = [](const std::string &accumulator, const WorkloadStep* step)
    {
        return accumulator.empty() ? step->toStringFull() : accumulator + "," + step->toStringFull();
    };

    const std::string stepsString = std::string("{ ") + std::accumulate(std::begin(steps), std::end(steps), std::string(), buildStringFromSteps) + std::string(" }");

    auto buildStringFromrIndexes = [](const std::string &accumulator, const DBIndex* index)
    {
        return accumulator.empty() ? index->toStringFull() : accumulator + "," + index->toStringFull();
    };

    const std::string rIndexesString = std::string("{ ") + std::accumulate(std::begin(rIndexes), std::end(rIndexes), std::string(), buildStringFromrIndexes) + std::string(" }");

    auto buildStringFromcIndexes = [](const std::string &accumulator, const DBIndexColumn* index)
    {
        return accumulator.empty() ? index->toStringFull() : accumulator + "," + index->toStringFull();
    };

    const std::string cIndexesString = std::string("{ ") + std::accumulate(std::begin(cIndexes), std::end(cIndexes), std::string(), buildStringFromcIndexes) + std::string(" }");

    auto buildStringFromCounters = [](const std::string &accumulator, const WorkloadCounters& total)
    {
        return accumulator.empty() ? total.toStringFull() : accumulator + "," + total.toStringFull();
    };

    const std::string totalCounterString = std::string("{ ") + std::accumulate(std::begin(totalCounters), std::end(totalCounters), std::string(), buildStringFromCounters) + std::string(" }");

    if (oneLine)
        return std::string(std::string("Worklad {") +
                           std::string(" .isColumnIndexMode = ") + std::to_string(isColumnIndexMode) +
                           std::string(" .steps = ") + stepsString +
                           std::string(" .sel = ") + std::to_string(sel) +
                           std::string(" .rIndexes = ") + rIndexesString +
                           std::string(" .cIndexes = ") + cIndexesString +
                           std::string(" .totalCounters = ") + totalCounterString +
                           std::string(" }"));
    else
        return std::string(std::string("Worklad {\n") +
                           std::string("\t.steps = ") + stepsString + std::string("\n") +
                           std::string("\t.sel = ") + std::to_string(sel) + std::string("\n") +
                           std::string("\t.rIndexes = ") + rIndexesString + std::string("\n") +
                           std::string("\t.cIndexes = ") + cIndexesString + std::string("\n") +
                           std::string("\t.totalCounters = ") + totalCounterString + std::string("\n") +
                           std::string("}"));
}

std::string WorkloadAdaptiveMerging::toStringFull(bool oneLine) const noexcept(true)
{
    auto buildStringFromSteps = [](const std::string &accumulator, const WorkloadStep* step)
    {
        return accumulator.empty() ? step->toStringFull() : accumulator + "," + step->toStringFull();
    };

    const std::string stepsString = std::string("{ ") + std::accumulate(std::begin(steps), std::end(steps), std::string(), buildStringFromSteps) + std::string(" }");

    auto buildStringFromrIndexes = [](const std::string &accumulator, const DBIndex* index)
    {
        return accumulator.empty() ? index->toStringFull() : accumulator + "," + index->toStringFull();
    };

    const std::string rIndexesString = std::string("{ ") + std::accumulate(std::begin(rIndexes), std::end(rIndexes), std::string(), buildStringFromrIndexes) + std::string(" }");

    auto buildStringFromcIndexes = [](const std::string &accumulator, const DBIndexColumn* index)
    {
        return accumulator.empty() ? index->toStringFull() : accumulator + "," + index->toStringFull();
    };

    const std::string cIndexesString = std::string("{ ") + std::accumulate(std::begin(cIndexes), std::end(cIndexes), std::string(), buildStringFromcIndexes) + std::string(" }");

    auto buildStringFromCounters = [](const std::string &accumulator, const WorkloadCounters& total)
    {
        return accumulator.empty() ? total.toStringFull() : accumulator + "," + total.toStringFull();
    };

    const std::string totalCounterString = std::string("{ ") + std::accumulate(std::begin(totalCounters), std::end(totalCounters), std::string(), buildStringFromCounters) + std::string(" }");

    auto buildStringFromStepCounters = [](const std::string &accumulator, const std::vector<WorkloadCounters>& step)
    {
        auto buildStringFromCounters = [](const std::string &accumulator, const WorkloadCounters& total)
        {
            return accumulator.empty() ? total.toStringFull() : accumulator + "," + total.toStringFull();
        };

        const std::string stepCounterString = std::string("{ ") + std::accumulate(std::begin(step), std::end(step), std::string(), buildStringFromCounters) + std::string(" }");
        return accumulator.empty() ? stepCounterString : accumulator + "," + stepCounterString;
    };

    const std::string stepsCounterString = std::string("{ ") + std::accumulate(std::begin(stepCounters), std::end(stepCounters), std::string(), buildStringFromStepCounters) + std::string(" }");


    if (oneLine)
        return std::string(std::string("Worklad {") +
                           std::string(" .isColumnIndexMode = ") + std::to_string(isColumnIndexMode) +
                           std::string(" .steps = ") + stepsString +
                           std::string(" .sel = ") + std::to_string(sel) +
                           std::string(" .rIndexes = ") + rIndexesString +
                           std::string(" .cIndexes = ") + cIndexesString +
                           std::string(" .totalCounters = ") + totalCounterString +
                           std::string(" .stepCounters = ") + stepsCounterString +
                           std::string(" }"));
    else
        return std::string(std::string("Worklad {\n") +
                           std::string("\t.isColumnIndexMode = ") + std::to_string(isColumnIndexMode) + std::string("\n") +
                           std::string("\t.steps = ") + stepsString + std::string("\n") +
                           std::string("\t.sel = ") + std::to_string(sel) + std::string("\n") +
                           std::string("\t.rIndexes = ") + rIndexesString + std::string("\n") +
                           std::string("\t.cIndexes = ") + cIndexesString + std::string("\n") +
                           std::string("\t.totalCounters = ") + totalCounterString + std::string("\n") +
                           std::string("\t.stepCounters = ") + stepsCounterString + std::string("\n") +
                           std::string("}"));
}