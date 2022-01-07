#include <workload/workload.hpp>
#include <logger/logger.hpp>

#include <numeric>

void Workload::aggregateCounters(WorkloadCounters& total, const WorkloadCounters& step) noexcept(true)
{
    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_TIME; id < WorkloadCounters::WORKLOAD_COUNTER_D_MAX_ITERATOR; ++id)
        total.pegCounter(id, step.getCounterValue(id));

    for (auto id = WorkloadCounters::WORKLOAD_COUNTER_RW_INSERT_TOTAL_OPERATIONS; id < WorkloadCounters::WORKLOAD_COUNTER_L_MAX_ITERATOR; ++id)
        total.pegCounter(id, step.getCounterValue(id));
}


Workload::Workload(const std::vector<DBIndex*>& indexes, const std::vector<WorkloadStep*>& steps)
: steps{steps}, rIndexes{indexes}, isColumnIndexMode{false}
{
    LOGGER_LOG_DEBUG("Workload created {}", toStringFull());
}

Workload::Workload(const std::vector<DBIndexColumn*>& indexes, const std::vector<WorkloadStep*>& steps)
: steps{steps}, cIndexes{indexes}, isColumnIndexMode{true}
{
    LOGGER_LOG_DEBUG("Workload created {}", toStringFull());
}

Workload::~Workload()
{
    for (size_t i = 0; i < steps.size(); ++i)
        delete steps[i];
}

Workload::Workload(const Workload& other)
{
    rIndexes = other.rIndexes;
    cIndexes = other.cIndexes;
    isColumnIndexMode = other.isColumnIndexMode;
    totalCounters = other.totalCounters;
    stepCounters = other.stepCounters;

    for (size_t i = 0; i < other.steps.size(); ++i)
        steps.push_back(other.steps[i]->clone());
}

Workload& Workload::operator=(const Workload& other)
{
    if (&other == this)
        return *this;

    rIndexes = other.rIndexes;
    cIndexes = other.cIndexes;
    isColumnIndexMode = other.isColumnIndexMode;
    totalCounters = other.totalCounters;
    stepCounters = other.stepCounters;

    for (size_t i = 0; i < other.steps.size(); ++i)
        steps.push_back(other.steps[i]->clone());

    return *this;
}

void Workload::addStep(WorkloadStep* step) noexcept(true)
{
    steps.push_back(step);
}

void Workload::addIndex(DBIndex* index) noexcept(true)
{
    rIndexes.push_back(index);
    isColumnIndexMode = false;
}

void Workload::addIndex(DBIndexColumn* index) noexcept(true)
{
    cIndexes.push_back(index);
    isColumnIndexMode = true;
}

void Workload::run() noexcept(true)
{
    size_t indexesSize = isColumnIndexMode == false ? rIndexes.size() : cIndexes.size();

    for (size_t i = 0; i < indexesSize; ++i)
    {
        WorkloadCounters totalStats;
        std::vector<WorkloadCounters> stepStats;

        // run steps for index(i)
        for (size_t j = 0; j < steps.size(); ++j)
        {
            if (isColumnIndexMode == false)
                steps[j]->setDbIndex(rIndexes[i]);
            else
                steps[j]->setDbIndex(cIndexes[i]);

            steps[j]->executeStep();

            const WorkloadCounters& stats = steps[j]->getCounters();
            stepStats.push_back(stats);
            aggregateCounters(totalStats, stats);
        }

        totalCounters.push_back(totalStats);
        stepCounters.push_back(stepStats);
    }
}

std::string Workload::toString(bool oneLine) const noexcept(true)
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
                           std::string(" .rIndexes = ") + rIndexesString +
                           std::string(" .cIndexes = ") + cIndexesString +
                           std::string(" .totalCounters = ") + totalCounterString +
                           std::string(" }"));
    else
        return std::string(std::string("Worklad {\n") +
                           std::string("\t.steps = ") + stepsString + std::string("\n") +
                           std::string("\t.rIndexes = ") + rIndexesString + std::string("\n") +
                           std::string("\t.cIndexes = ") + cIndexesString + std::string("\n") +
                           std::string("\t.totalCounters = ") + totalCounterString + std::string("\n") +
                           std::string("}"));
}

std::string Workload::toStringFull(bool oneLine) const noexcept(true)
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
                           std::string(" .rIndexes = ") + rIndexesString +
                           std::string(" .cIndexes = ") + cIndexesString +
                           std::string(" .totalCounters = ") + totalCounterString +
                           std::string(" .stepCounters = ") + stepsCounterString +
                           std::string(" }"));
    else
        return std::string(std::string("Worklad {\n") +
                           std::string("\t.isColumnIndexMode = ") + std::to_string(isColumnIndexMode) + std::string("\n") +
                           std::string("\t.steps = ") + stepsString + std::string("\n") +
                           std::string("\t.rIndexes = ") + rIndexesString + std::string("\n") +
                           std::string("\t.cIndexes = ") + cIndexesString + std::string("\n") +
                           std::string("\t.totalCounters = ") + totalCounterString + std::string("\n") +
                           std::string("\t.stepCounters = ") + stepsCounterString + std::string("\n") +
                           std::string("}"));
}