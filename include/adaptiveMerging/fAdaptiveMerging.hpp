#ifndef F_ADAPTIVE_MERGING_HPP
#define F_ADAPTIVE_MERGING_HPP

#include <adaptiveMerging/adaptiveMerging.hpp>

class FAdaptiveMerging : public AdaptiveMerging
{
public:
    /**
     * @brief Create FAdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return FAdaptiveMerging object
     */
    FAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
     * @brief Create FAdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return FAdaptiveMerging object
     */
    FAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new FAdaptiveMerging(*this);
    }

    virtual ~FAdaptiveMerging() = default;
    FAdaptiveMerging() = default;
    FAdaptiveMerging(const FAdaptiveMerging&) = default;
    FAdaptiveMerging& operator=(const FAdaptiveMerging&) = default;
    FAdaptiveMerging(FAdaptiveMerging &&) = default;
    FAdaptiveMerging& operator=(FAdaptiveMerging &&) = default;
};

#endif