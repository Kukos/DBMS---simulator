#ifndef ADAPTIVE_MERGING_FRAMEWORK_HPP
#define ADAPTIVE_MERGING_FRAMEWORK_HPP

#include <index/dbIndex.hpp>
#include <observability/adaptiveMergingCounters.hpp>

#include <string>
#include <random>

class AdaptiveMergingFramework : public DBIndex
{
public:
    class AMUnsortedMemoryManager
    {
    public:
        enum AMUnsortedMemoryInvalidation
        {
            AM_UNSORTED_MEMORY_INVALIDATION_OVERWRITE,
            AM_UNSORTED_MEMORY_INVALIDATION_FLAG,
            AM_UNSORTED_MEMORY_INVALIDATION_BITMAP,
            AM_UNSORTED_MEMORY_INVALIDATION_JOURNAL
        };
    protected:
        AdaptiveMergingCounters counters;

        size_t numEntries;
        size_t recordSize;
        enum AMUnsortedMemoryInvalidation invalidationType;

    public:
        /**
         * @brief Construct a new AMUnsortedMemoryManager object
         *
         * @param[in] numEntries - how many entries go int unsorted area
         * @param[in] recordSize - entry (record) size
         * @param[in] invalidationType - invalidation type
         *
         * @return AMUnsortedMemoryManager object
         */
        AMUnsortedMemoryManager(size_t numEntries, size_t recordSize, enum AMUnsortedMemoryInvalidation invalidationType);

        /**
        * @brief Virtual constructor idiom implemented as clone function. This function creates new AMUnsortedMemoryManager
        *
        * @return new AMUnsortedMemoryManager
        */
        virtual AMUnsortedMemoryManager* clone() const noexcept(true)
        {
            return new AMUnsortedMemoryManager(*this);
        }

        /**
         * @brief Get the Invalidation Type object
         *
         * @return enum AMUnsortedMemoryInvalidation
         */
        virtual enum AMUnsortedMemoryInvalidation getInvalidationType() const noexcept(true)
        {
            return invalidationType;
        }

        /**
         * @brief Get the Counters object
         *
         * @return const AdaptiveMergingCounters&
         */
        virtual const AdaptiveMergingCounters& getCounters() const noexcept(true)
        {
            return counters;
        }

        /**
         * @brief Get number of entries
         *
         * @return number of entries in AdaptiveMergingFramework
         */
        virtual size_t getNumEntries() const noexcept(true)
        {
           return numEntries;
        }

        /**
         * @brief get size of whole record
         *
         * @return size of record
         */
        virtual size_t getRecordSize() const noexcept(true)
        {
            return recordSize;
        }

        /**
         * @brief Load entries from unsorted part
         *
         * @param[in] disk - pointer to disk
         * @param[in] numEntries - how many entries load
         * @return time
         */
        virtual double loadEntries(Disk *disk, size_t numEntries) noexcept(true)
        {
            (void)disk;
            (void)numEntries;

            this->numEntries -= std::min(numEntries, this->numEntries);

            return 0.0;
        }

        /**
         * @brief Invalid entries from unsorted part
         *
         * @param[in] disk - pointer to disk
         * @param[in] numEntries - how many entries invalid
         * @return time
         */
        virtual double invalidEntries(Disk* disk, size_t numEntries, size_t nodeSize) noexcept(true);

        /**
         * @brief Created brief snapshot of AMUnsortedMemoryManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of AMUnsortedMemoryManager
         */
        virtual std::string toString(bool oneLine = true) const noexcept(true);

        /**
         * @brief Created full snapshot of AMUnsortedMemoryManager as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of AMUnsortedMemoryManager
         */
        virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

        virtual ~AMUnsortedMemoryManager() = default;
        AMUnsortedMemoryManager() = default;
        AMUnsortedMemoryManager(const AMUnsortedMemoryManager&) = default;
        AMUnsortedMemoryManager& operator=(const AMUnsortedMemoryManager&) = default;
        AMUnsortedMemoryManager(AMUnsortedMemoryManager &&) = default;
        AMUnsortedMemoryManager& operator=(AMUnsortedMemoryManager &&) = default;

        friend class AdaptiveMergingFramework;
    };
protected:
    const static size_t seed = 235111741; // euler lucky numbers 2, 3, 5, 11, 7, 41
    std::unique_ptr<DBIndex> index;
    std::unique_ptr<AMUnsortedMemoryManager> memoryManager;
    size_t startingEntries;

    std::mt19937 rng;

    /**
     * @brief Calculate in a random (depends on index size) how many entries we can load from index and how many from unsorted memory
     *
     * @param[in] numEntries - total entries to load from AMF
     * @return std::pair<entriesFromIndex, entriesFromUnsorted>
     */
    virtual std::pair<size_t, size_t> splitLoadForIndexAndUnsortedPart(size_t numEntries) noexcept(true);
public:

    /**
     * @brief Create AdaptiveMergingFramework
     *
     * @param[in] name - Index name
     * @param[in] index - pointer to DBIndex
     * @param[in] manager - pointer to UnsortedMemoryManager
     * @param[in] startingEntries - starting unsorted entries in Table
     *
     * @return AdaptiveMergingFramework object
     */
    AdaptiveMergingFramework(const char* name, DBIndex* index, AMUnsortedMemoryManager* manager, size_t startingEntries);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new AdaptiveMergingFramework
    *
    * @return new AdaptiveMergingFramework
    */
    virtual AdaptiveMergingFramework* clone() const noexcept(true) = 0;

    /**
     * @brief Reset non-const values to default value
     *
     */
    virtual void resetState() noexcept(true)
    {
        index->resetState();
        memoryManager.reset(new AMUnsortedMemoryManager(startingEntries, getRecordSize(), memoryManager->getInvalidationType()));
    }

    /**
     * @brief Created brief snapshot of AdaptiveMergingFramework as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of AdaptiveMergingFramework
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of AdaptiveMergingFramework as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of AdaptiveMergingFramework
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true);

    /**
     * @brief Get the Name object
     *
     * @return const char*
     */
    virtual const char* getName() const noexcept(true)
    {
        return name;
    }

    /**
     * @brief Get Disk as a const reference
     *
     * @return Const reference to disk
     */
    virtual const Disk& getDisk() const noexcept(true)
    {
        return index->getDisk();
    }

    /**
     * @brief Get the Memory Manager object
     *
     * @return const AMUnsortedMemoryManager&
     */
    virtual const AMUnsortedMemoryManager& getMemoryManager() const noexcept(true)
    {
        return *memoryManager;
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, double> getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true);

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, long> getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true);
    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true);

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true);

    /**
     * @brief Reset all counter
     */
    virtual void resetAllCounters() noexcept(true);

    /**
     * @brief Get number of entries
     *
     * @return number of entries in AdaptiveMergingFramework
     */
    virtual size_t getNumEntries() const noexcept(true)
    {
        return index->getNumEntries() + memoryManager->getNumEntries();
    }

    /**
     * @brief Get size of key
     *
     * @return size of key
     */
    virtual size_t getKeySize() const noexcept(true)
    {
        return index->getKeySize();
    }

    /**
     * @brief Get size of data
     *
     * @return size of data
     */
    virtual size_t getDataSize() const noexcept(true)
    {
        return index->getDataSize();
    }

    /**
     * @brief get size of whole record
     *
     * @return size of record
     */
    virtual size_t getRecordSize() const noexcept(true)
    {
        return index->getRecordSize();
    }

    /**
     * @brief Check if bulkload operation is supported
     *
     * @return true if bulkload is supported
     *         false otherwise
     */
    virtual bool isBulkloadSupported() const noexcept(true)
    {
        return index->isBulkloadSupported();
    }

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
    virtual double insertEntries(size_t numOperations = 1) noexcept(true) = 0;

    /**
     * @brief Insert new entries to the AdaptiveMergingFramework by using bulkload
     *        If bulkload is not supported this function does nothing and returns 0.0
     *
     * @param[in] numEntries - entries to insert via bulkload
     *
     * @return time needed to evaluates the operation
     */
    virtual double bulkloadEntries(size_t numEntries = 1) noexcept(true) = 0;

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
    virtual double deleteEntries(size_t numOperations = 1) noexcept(true) = 0;

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
    virtual double findPointEntries(size_t numOperations = 1)  noexcept(true) = 0;

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
    virtual double findPointEntries(double selectivity, size_t numOperations = 1) noexcept(true) = 0;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] numEntries - number of entries to seek
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(size_t numEntries, size_t numOperations = 1)  noexcept(true) = 0;

    /**
     * @brief Find entries from the AdaptiveMergingFramework using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(double selectivity, size_t numOperations = 1) noexcept(true) = 0;

    /**
     * @brief Fake the insert of numEntries entries to create a topology in fastest way.
     *        This should be used only to test non-empty index in workloads, so this is a total fakeout.
     *        No low-level simulation is done here. We are simulating only insert index code without involving disk simulator.
     *        Counters wouldnt be pegged
     *
     * @param[in] numEntries - how entries to insert to create a topology
     */
    virtual void createTopologyAfterInsert(size_t numEntries = 1) noexcept(true) = 0;

    virtual ~AdaptiveMergingFramework() = default;
    AdaptiveMergingFramework() = default;

    AdaptiveMergingFramework(const AdaptiveMergingFramework&);
    AdaptiveMergingFramework& operator=(const AdaptiveMergingFramework&);

    AdaptiveMergingFramework(AdaptiveMergingFramework &&) = default;
    AdaptiveMergingFramework& operator=(AdaptiveMergingFramework &&) = default;
};

#endif