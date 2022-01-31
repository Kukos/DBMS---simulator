#include <adaptiveMerging/pcmAdaptiveMerging.hpp>

PCMAdaptiveMerging::PCMAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize)
: AdaptiveMerging(name, index, new AMPartitionManager(startingEntries, index->getRecordSize(), partitionSize, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL), startingEntries)
{
    LOGGER_LOG_DEBUG("PCMAdaptiveMerging created {}", toStringFull());
}

PCMAdaptiveMerging::PCMAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize)
: PCMAdaptiveMerging("PCMAdaptiveMerging", index, startingEntries, partitionSize)
{

}