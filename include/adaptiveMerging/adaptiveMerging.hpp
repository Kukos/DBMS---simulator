#ifndef ADAPTIVE_MERGING_HPP
#define ADAPTIVE_MERGING_HPP

#include <adaptiveMerging/adaptiveMergingFramework.hpp>

#include <vector>

class AdaptiveMerging : public AdaptiveMergingFramework
{
public:
    class AMPartitionManager : public AdaptiveMergingFramework::AMUnsortedMemoryManager
    {
    public:
        class AMPartition
        {
        protected:
            size_t numEntries;
        public:
            /**
             * @brief Construct a new AMPartition object
             *
             * @param[in] numEntries - entries in partition
             *
             * @return AMPartition object
             */
            AMPartition(size_t numEntries);

            /**
             * @brief Get the Num Entries object
             *
             * @return entries in partition
             */
            virtual size_t getNumEntries() const noexcept(true)
            {
                return numEntries;
            }

            /**
             * @brief Created brief snapshot of AMPartition as a string
             *
             * @param[in] oneLine - create string as 1 line or not? By default Yes
             * @return brief edscription of AMPartition
             */
            virtual std::string toString(bool oneLine = true) const noexcept(true);

            /**
             * @brief Created full snapshot of AMPartition as a string
             *
             * @param[in] oneLine - create string as 1 line or not? By default Yes
             * @return Full edscription of AMPartition
             */
            virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

            virtual ~AMPartition() = default;
            AMPartition() = default;
            AMPartition(const AMPartition&) = default;
            AMPartition& operator=(const AMPartition&) = default;
            AMPartition(AMPartition &&) = default;
            AMPartition& operator=(AMPartition &&) = default;

            friend class AMPartitionManager;
        };

    protected:
        std::vector<AMPartition> partitions;
        size_t partitionSize;
    public:
        /**
         * @brief Construct a new AMUnsortedMemoryManager object
         *
         * @param numEntries - how many entries go int unsorted area
         * @param recordSize - entry (record) size
         *
         * @param invalidationType - invalidation type
         */
        AMPartitionManager(size_t numEntries, size_t recordSize, size_t partitionSize, enum AMUnsortedMemoryInvalidation invalidationType);

        /**
         * @brief Get the Partitions Ref object
         *
         * @return const std::vector<AMPartition>&
         */
        virtual const std::vector<AMPartition>& getPartitionsRef() const noexcept(true)
        {
            return partitions;
        }

        /**
         * @brief Created brief snapshot of AMPartitionManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of AMPartitionManager
         */
        virtual std::string toString(bool oneLine = true) const noexcept(true) override;

        /**
         * @brief Created full snapshot of AMPartitionManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of AMPartitionManager
         */
        virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

        /**
        * @brief Virtual constructor idiom implemented as clone function. This function creates new AMUnsortedMemoryManager
        *
        * @return new AMUnsortedMemoryManager
        */
        virtual AMUnsortedMemoryManager* clone() const noexcept(true)
        {
            return new AMPartitionManager(*this);
        }

        /**
         * @brief Set entries to 0 for all partitions and set numEntries to 0
         *        This is only logical layer, so cost nothing
         *
         */
        virtual void cleanPartitions() noexcept(true);

        /**
         * @brief Load entries from unsorted part
         *
         * @param[in] disk - pointer to disk
         * @param[in] numEntries - how many entries load
         * @return time
         */
        virtual double loadEntries(Disk *disk, size_t numEntries) noexcept(true) override;

        virtual ~AMPartitionManager() = default;
        AMPartitionManager() = default;
        AMPartitionManager(const AMPartitionManager&) = default;
        AMPartitionManager& operator=(const AMPartitionManager&) = default;
        AMPartitionManager(AMPartitionManager &&) = default;
        AMPartitionManager& operator=(AMPartitionManager &&) = default;

        friend class AdaptiveMerging;
    };

protected:
    virtual double findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true);
    virtual double deleteEntriesHelper(size_t numOperations) noexcept(true);
    static size_t constexpr copyTreshold = 1000;
public:

    /**
     * @brief Create AdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return AdaptiveMerging object
     */
    AdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
     * @brief Create AdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     *
     * @return AdaptiveMerging object
     */
    AdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize);

    /**
     * @brief Create AdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] manager - memory manager
     * @param[in] startingEntries - starting unsorted entries in Table
     *
     * @return AdaptiveMerging object
     */
    AdaptiveMerging(const char* name, DBIndex* index, AMPartitionManager* manager, size_t startingEntries);

    /**
     * @brief Create AdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] manager - memory manager
     * @param[in] startingEntries - starting unsorted entries in Table
     *
     * @return AdaptiveMerging object
     */
    AdaptiveMerging(DBIndex* index, AMPartitionManager* manager, size_t startingEntries);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new AdaptiveMerging(*this);
    }

    /**
     * @brief Created brief snapshot of AdaptiveMergingFramework as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of AdaptiveMergingFramework
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of AdaptiveMergingFramework as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of AdaptiveMergingFramework
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Insert new entries to the AdaptiveMergingFramework
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double insertEntries(size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Insert new entries to the AdaptiveMergingFramework by using bulkload
     *        If bulkload is not supported this function does nothing and returns 0.0
     *
     * @param[in] numEntries - entries to insert via bulkload
     *
     * @return time needed to evaluates the operation
     */
    virtual double bulkloadEntries(size_t numEntries = 1) noexcept(true) override;

    /**
     * @brief Delete entries from the AdaptiveMergingFramework
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double deleteEntries(size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(size_t numOperations = 1)  noexcept(true) override;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(double selectivity, size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] numEntries - number of entries to seek
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(size_t numEntries, size_t numOperations = 1)  noexcept(true) override;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(double selectivity, size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Fake the insert of numEntries entries to create a topology in fastest way.
     *        This should be used only to test non-empty index in workloads, so this is a total fakeout.
     *        No low-level simulation is done here. We are simulating only insert index code without involving disk simulator.
     *        Counters wouldnt be pegged
     *
     * @param[in] numEntries - how entries to insert to create a topology
     */
    virtual void createTopologyAfterInsert(size_t numEntries = 1) noexcept(true) override;

    virtual ~AdaptiveMerging() = default;
    AdaptiveMerging() = default;
    AdaptiveMerging(const AdaptiveMerging&) = default;
    AdaptiveMerging& operator=(const AdaptiveMerging&) = default;
    AdaptiveMerging(AdaptiveMerging &&) = default;
    AdaptiveMerging& operator=(AdaptiveMerging &&) = default;
};

#endif