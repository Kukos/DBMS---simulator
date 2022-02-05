#ifndef WORKLOAD_LIB_HPP
#define WORKLOAD_LIB_HPP

// This file contains all includes required to make own workload from app
#include <workload/workloadStep.hpp>
#include <workload/workloadStepInsert.hpp>
#include <workload/workloadStepBulkload.hpp>
#include <workload/workloadStepDelete.hpp>
#include <workload/workloadStepPSearch.hpp>
#include <workload/workloadStepRSearch.hpp>

#include <workload/workload.hpp>
#include <workload/workloadAdaptiveMerging.hpp>

#include <workload/analyzer/analyzerStep.hpp>
#include <workload/analyzer/analyzerStepTotalTime.hpp>
#include <workload/analyzer/analyzerStepTotalTimeNormalized.hpp>
#include <workload/analyzer/analyzerStepTotalWearout.hpp>
#include <workload/analyzer/analyzerStepTotalWearoutNormalized.hpp>
#include <workload/analyzer/analyzerStepQueryTime.hpp>
#include <workload/analyzer/analyzerStepQueryTimeNormalized.hpp>
#include <workload/analyzer/analyzerStepQueryWearout.hpp>
#include <workload/analyzer/analyzerStepQueryWearoutNormalized.hpp>

#include <workload/analyzer/analyzer.hpp>
#include <workload/analyzer/analyzerConsole.hpp>
#include <workload/analyzer/analyzerFile.hpp>

#include <observability/counterManager.hpp>
#include <observability/indexCounters.hpp>
#include <observability/memoryCounters.hpp>
#include <observability/workloadCounters.hpp>

#endif