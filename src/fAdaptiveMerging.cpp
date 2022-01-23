#include <adaptiveMerging/fAdaptiveMerging.hpp>

FAdaptiveMerging::FAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize)
: AdaptiveMerging(name, index, new AMPartitionManager(startingEntries, index->getRecordSize(), partitionSize, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_FLAG), startingEntries)
{
    LOGGER_LOG_DEBUG("FLaggedAdaptiveMerging created {}", toStringFull());
}

FAdaptiveMerging::FAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize)
: FAdaptiveMerging("FlaggedAdaptiveMerging", index, startingEntries, partitionSize)
{

}