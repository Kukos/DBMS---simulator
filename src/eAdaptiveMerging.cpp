#include <adaptiveMerging/eAdaptiveMerging.hpp>

EAdaptiveMerging::EAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize)
: AdaptiveMerging(name, index, new AMPartitionManager(startingEntries, index->getRecordSize(), partitionSize, AdaptiveMergingFramework::AMUnsortedMemoryManager::AM_UNSORTED_MEMORY_INVALIDATION_BITMAP), startingEntries)
{
    LOGGER_LOG_DEBUG("ExtendendAdaptiveMerging created {}", toStringFull());
}

EAdaptiveMerging::EAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize)
: EAdaptiveMerging("ExtendendAdaptiveMerging", index, startingEntries, partitionSize)
{

}