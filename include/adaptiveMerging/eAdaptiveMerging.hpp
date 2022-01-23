#ifndef E_ADAPTIVE_MERGING_HPP
#define E_ADAPTIVE_MERGING_HPP

#include <adaptiveMerging/adaptiveMerging.hpp>

class EAdaptiveMerging : public AdaptiveMerging
{
public:
    /**
     * @brief Create EAdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return EAdaptiveMerging object
     */
    EAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
     * @brief Create EAdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return EAdaptiveMerging object
     */
    EAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new EAdaptiveMerging(*this);
    }

    virtual ~EAdaptiveMerging() = default;
    EAdaptiveMerging() = default;
    EAdaptiveMerging(const EAdaptiveMerging&) = default;
    EAdaptiveMerging& operator=(const EAdaptiveMerging&) = default;
    EAdaptiveMerging(EAdaptiveMerging &&) = default;
    EAdaptiveMerging& operator=(EAdaptiveMerging &&) = default;
};

#endif