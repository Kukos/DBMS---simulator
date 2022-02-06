#ifndef LAZY_ADAPTIVE_MERGING_HPP
#define LAZY_ADAPTIVE_MERGING_HPP

#include <adaptiveMerging/adaptiveMergingFramework.hpp>

class LazyAdaptiveMerging : public AdaptiveMergingFramework
{
public:
     class LAMPartitionManager : public AdaptiveMergingFramework::AMUnsortedMemoryManager
     {
    public:
        class LAMPartition
        {
        public:
            class LAMPartitionLEB
            {
            protected:
                size_t curValidEntries;
                size_t curInvalidEntries;
                size_t maxEntries;
            public:
                /**
                 * @brief Construct a new LAMPartitionLEB object
                 *
                 * @param[in] maxEntries - max entries per LEB
                 */
                LAMPartitionLEB(size_t maxEntries);

                /**
                 * @brief Construct a new LAMPartitionLEB object
                 *
                 * @param entries - current entries in LEB
                 * @param maxEntries - max entries in LEB
                 */
                LAMPartitionLEB(size_t entries, size_t maxEntries);

                /**
                 * @brief Created brief snapshot of LAMPartitionLEB as a string
                 *
                 * @param[in] oneLine - create string as 1 line or not? By default Yes
                 * @return brief edscription of LAMPartitionLEB
                 */
                virtual std::string toString(bool oneLine = true) const noexcept(true);

                /**
                 * @brief Created full snapshot of LAMPartitionLEB as a string
                 *
                 * @param[in] oneLine - create string as 1 line or not? By default Yes
                 * @return Full edscription of LAMPartitionLEB
                 */
                virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

                /**
                 * @brief Calculate usage of leb. 1.0 means full, 0.0 means empty
                 *
                 * @return usage
                 */
                virtual double usage() const noexcept(true);

                /**
                 * @brief Read Entries from LEB
                 *
                 * @param[in] entries - entries to read
                 */
                virtual void readEntries(size_t entries)
                {
                    curValidEntries -= entries;
                    curInvalidEntries += entries;
                }

                size_t getCurValidEntries() const noexcept(true)
                {
                    return curValidEntries;
                }

                size_t getCurInvalidEntries() const noexcept(true)
                {
                    return curInvalidEntries;
                }

                size_t getMaxEntries() const noexcept(true)
                {
                    return maxEntries;
                }

                virtual ~LAMPartitionLEB() = default;
                LAMPartitionLEB() = default;
                LAMPartitionLEB(const LAMPartitionLEB&) = default;
                LAMPartitionLEB& operator=(const LAMPartitionLEB&) = default;
                LAMPartitionLEB(LAMPartitionLEB &&) = default;
                LAMPartitionLEB& operator=(LAMPartitionLEB &&) = default;

                friend class LAMPartition;
            };
        protected:
            size_t numEntries;
            size_t maxEntriesInBlock;
            std::vector<LAMPartitionLEB> blocks;
        public:

            /**
             * @brief Construct a new LAMPartition object
             *
             * @param[in] entries - entries in partition at starting point
             * @param[in] maxEntriesInBlock - max entries in LEB
             */
            LAMPartition(size_t entries, size_t maxEntriesInBlock);

            /**
             * @brief Created brief snapshot of LAMPartition as a string
             *
             * @param[in] oneLine - create string as 1 line or not? By default Yes
             * @return brief edscription of LAMPartition
             */
            virtual std::string toString(bool oneLine = true) const noexcept(true);

            /**
             * @brief Created full snapshot of LAMPartition as a string
             *
             * @param[in] oneLine - create string as 1 line or not? By default Yes
             * @return Full edscription of LAMPartition
             */
            virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

            /**
             * @brief Get the blocks Ref object
             *
             * @return const std::vector<LAMPartitionLEB>&
             */
            virtual const std::vector<LAMPartitionLEB>& getBlocksRef() const noexcept(true)
            {
                return blocks;
            }

            /**
             * @brief Read entries from LEB
             *
             * @param[in] entries - entries to read from leb
             * @param[in] lebPos - leb position in vector
             */
            virtual void readEntriesFromLebPos(size_t entries, size_t lebPos) noexcept(true)
            {
                blocks[lebPos].readEntries(entries);
            }

            /**
             * @brief Reset (clear) blocks
             *
             */
            virtual void reset() noexcept(true);

            /**
             * @brief Add Leb to blocks vector
             *
             * @param[in] leb LEB to add
             */
            virtual void addLeb(const LAMPartitionLEB& leb) noexcept(true);

            virtual ~LAMPartition() = default;
            LAMPartition() = default;
            LAMPartition(const LAMPartition&) = default;
            LAMPartition& operator=(const LAMPartition&) = default;
            LAMPartition(LAMPartition &&) = default;
            LAMPartition& operator=(LAMPartition &&) = default;

            friend class LAMPartitionManager;
        };
    protected:
        std::vector<LAMPartition> partitions;
        size_t partitionSize;
        size_t maxEntriesInParition;
        size_t maxEntriesInBlock;
        size_t reorganizationMaxBlocks;
        size_t reorganizationBlocksTreshold;
        double lebUsageTreshold;
        size_t reorganizationCounter;
        std::mt19937* rng;
    public:

        /**
         * @brief Construct a new LAMPartitionManager object
         *
         * @param[in] numEntries - how many entries go into unsorted area
         * @param[in] recordSize - entry (record) size in bytes
         * @param[in] partitionSize - size of the partition in bytes
         * @param[in] blockSize - size of 1 ssd block in bytes
         * @param[in] reorganizationMaxBlocks - how many lebs can be used in reorganize algorithm
         * @param[in] reorganizationBlocksTreshold - minimum triggered lebs to make a reroganization (how many lebs can we utilize by reorganization)
        * @param[in] lebUsageTreshold - minimum usage for leb to be marked as "toReorganize"
         * @param[in] invalidationType - invalidation type
         */
        LAMPartitionManager(size_t numEntries, size_t recordSize, size_t partitionSize, size_t blockSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold, enum AMUnsortedMemoryInvalidation invalidationType);

        /**
        * @brief Virtual constructor idiom implemented as clone function. This function creates new AMUnsortedMemoryManager
        *
        * @return new AMUnsortedMemoryManager
        */
        virtual AMUnsortedMemoryManager* clone() const noexcept(true)
        {
            return new LAMPartitionManager(*this);
        }

        virtual void setRng(std::mt19937* rng) noexcept(true)
        {
            this->rng = rng;
        }

        /**
         * @brief Get the Partitions Ref object
         *
         * @return const std::vector<AMPartition>&
         */
        virtual const std::vector<LAMPartition>& getPartitionsRef() const noexcept(true)
        {
            return partitions;
        }

        /**
         * @brief Delete (clean) partitions
         *
         */
        virtual void cleanPartitions() noexcept(true);

        /**
         * @brief Get the Number Of Rerganization object
         *
         * @return size_t
         */
        virtual size_t getNumberOfRerganization() const noexcept(true)
        {
            return reorganizationCounter;
        }

        /**
         * @brief Get the Number Of Lebs object
         *
         * @return size_t
         */
        virtual size_t getNumberOfLebs() const noexcept(true);

        /**
         * @brief Load entries from unsorted part
         *
         * @param[in] disk - pointer to disk
         * @param[in] numEntries - how many entries load
         * @return time
         */
        virtual double loadEntries(Disk *disk, size_t numEntries) noexcept(true) override;

        /**
         * @brief Check is LEB should be deleted
         *
         * @param[in] leb - ref to LEB
         * @return true if usage is below treshold
         * @return false if usage is above or equal to treshold
         */
        virtual bool isLebToDelete(const LAMPartition::LAMPartitionLEB& leb) const noexcept(true);

        /**
         * @brief Check if Paritions should be reorganize
         *
         * @return true if should
         * @return false if should not
         */
        virtual bool canReorganizePartitions() const noexcept(true);

        /**
         * @brief Reorganize partitions
         *
         * @param[in] disk - pointer to the Disk
         * @param[in] recordSize - size of the database record
         *
         * @return time consumed by reorganization
         */
        virtual double reorganizePartitions(Disk* disk, size_t recordSize) noexcept(true);

        /**
         * @brief Created brief snapshot of LAMPartitionManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of LAMPartitionManager
         */
        virtual std::string toString(bool oneLine = true) const noexcept(true) override;

        /**
         * @brief Created full snapshot of LAMPartitionManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of LAMPartitionManager
         */
        virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

        virtual ~LAMPartitionManager() = default;
        LAMPartitionManager() = default;
        LAMPartitionManager(const LAMPartitionManager&) = default;
        LAMPartitionManager& operator=(const LAMPartitionManager&) = default;
        LAMPartitionManager(LAMPartitionManager &&) = default;
        LAMPartitionManager& operator=(LAMPartitionManager &&) = default;

        friend class LazyAdaptiveMerging;
     };

protected:
    static size_t constexpr copyTreshold = 1000;

    double deleteEntriesHelper(size_t numOperations) noexcept(true);
    double findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true);

public:

    /**
     * @brief Create LazyAdaptiveMerging
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     * @param[in] reorganizationMaxBlocks - how many lebs can be used in reorganize algorithm
     * @param[in] reorganizationBlocksTreshold - minimum triggered lebs to make a reroganization (how many lebs can we utilize by reorganization)
     * @param[in] lebUsageTreshold - minimum usage for leb to be marked as "toReorganize"
     *
     * @return LazyAdaptiveMerging object
     */
    LazyAdaptiveMerging(const char* name, DBIndex* index, size_t startingEntries, size_t partitionSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold);

    /**
     * @brief Create LazyAdaptiveMerging
     *
     * @param[in] index - pointer to DBIndex
     * @param[in] startingEntries - starting unsorted entries in Table
     * @param[in] partitionSize - size of single partition in bytes
     * @param[in] reorganizationMaxBlocks - how many lebs can be used in reorganize algorithm
     * @param[in] reorganizationBlocksTreshold - minimum triggered lebs to make a reroganization
     * @param[in] lebUsageTreshold - minimum usage for leb to be marked as "toReorganize"
     *
     * @return LazyAdaptiveMerging object
     */
    LazyAdaptiveMerging(DBIndex* index, size_t startingEntries, size_t partitionSize, size_t reorganizationMaxBlocks, size_t reorganizationBlocksTreshold, double lebUsageTreshold);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) override
    {
        return new LazyAdaptiveMerging(*this);
    }

    /**
     * @brief Created brief snapshot of LazyAdaptiveMerging as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of LazyAdaptiveMerging
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of LazyAdaptiveMerging as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of LazyAdaptiveMerging
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

    virtual ~LazyAdaptiveMerging() = default;
    LazyAdaptiveMerging() = default;
    LazyAdaptiveMerging(const LazyAdaptiveMerging&) = default;
    LazyAdaptiveMerging& operator=(const LazyAdaptiveMerging&) = default;
    LazyAdaptiveMerging(LazyAdaptiveMerging &&) = default;
    LazyAdaptiveMerging& operator=(LazyAdaptiveMerging &&) = default;
};

#endif