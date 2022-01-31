#ifndef PCM_ADAPTIVE_MERGING_HPP
#define PCM_ADAPTIVE_MERGING_HPP

#include <adaptiveMerging/adaptiveMerging.hpp>

class PCMAdaptiveMerging : public AdaptiveMerging
{
public:
    /**
     * @brief Create PCMAdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return PCMAdaptiveMerging object
     */
    PCMAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
     * @brief Create PCMAdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return PCMAdaptiveMerging object
     */
    PCMAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new PCMAdaptiveMerging(*this);
    }

    virtual ~PCMAdaptiveMerging() = default;
    PCMAdaptiveMerging() = default;
    PCMAdaptiveMerging(const PCMAdaptiveMerging&) = default;
    PCMAdaptiveMerging& operator=(const PCMAdaptiveMerging&) = default;
    PCMAdaptiveMerging(PCMAdaptiveMerging &&) = default;
    PCMAdaptiveMerging& operator=(PCMAdaptiveMerging &&) = default;
};

#endif