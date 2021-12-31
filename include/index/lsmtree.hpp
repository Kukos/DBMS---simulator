#ifndef LSMTREE_HPP
#define LSMTREE_HPP

#include <index/dbIndex.hpp>
#include <vector>

class LSMTree : public DBIndex
{
public:
    class LSMLvl
    {
    private:
        size_t sizeInBytes;
        size_t sizeInNodes;
        size_t numEntries;
        size_t numEntriesToDelete;
        size_t maxEntries;
        size_t lvl;

        bool isFull() const noexcept(true);
        bool willBeFullAfterMerge(const LSMLvl& lvl);

        friend class LSMTree;
    public:
        LSMLvl(size_t sizeInBytes, size_t recordSize, size_t lvl, size_t nodeSize);

        size_t getSize() const noexcept(true)
        {
            return sizeInBytes;
        }

        size_t getNumOfNodes() const noexcept(true)
        {
            return sizeInNodes;
        }

        size_t getNumEntries() const noexcept(true)
        {
            return numEntries;
        }

        size_t getNumEntriesToDelete() const noexcept(true)
        {
            return numEntriesToDelete;
        }

        size_t getMaxEntries() const noexcept(true)
        {
            return maxEntries;
        }

        size_t getLvl() const noexcept(true)
        {
            return lvl + 1;
        }

        /**
         * @brief Created brief snapshot of LSMLvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of LSMLvl
         */
        std::string toString(bool oneLine = true) const noexcept(true);

        /**
         * @brief Created full snapshot of LSMLvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of LSMLvl
         */
        std::string toStringFull(bool oneLine = true) const noexcept(true);

        virtual ~LSMLvl() = default;
        LSMLvl() = default;
        LSMLvl(const LSMLvl&) = default;
        LSMLvl& operator=(const LSMLvl&) = default;
        LSMLvl(LSMLvl &&) = default;
        LSMLvl& operator=(LSMLvl &&) = default;
    };

    enum BulkloadFeatureMode
    {
        BULKLOAD_FEATURE_OFF,
        BULKLOAD_ACCORDING_TO_CURRENT_CAPACITY,
        BULKLOAD_ACCORDING_TO_MAX_CAPACITY,
    };

protected:
    size_t nodeSize;
    size_t lvlRatio;

    LSMLvl bufferTree;
    std::vector<LSMLvl> levels;

    enum BulkloadFeatureMode bulkloadMode;

private:
    double insertIntoBufferTree(size_t entries) noexcept(true);
    double deleteFromBufferTree(size_t entries) noexcept(true);
    void addLevel() noexcept(true);

    double mergeBufferTree() noexcept(true);
    double mergeLevels(size_t upperLvl, size_t lowerLvl) noexcept(true);
    double addEntriesToLevel(size_t lvl, size_t entries) noexcept(true);

    double insertEntriesHelper(size_t numOperations) noexcept(true);
    double deleteEntriesHelper(size_t numOperations) noexcept(true);
    double findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true);
    double bulkloadEntriesHelperCurrentCapacity(size_t numEntries) noexcept(true);
    double bulkloadEntriesHelperMaxCapacity(size_t numEntries) noexcept(true);

public:
    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndex
    *
    * @return new DBIndex
    */
    DBIndex* clone() const noexcept(true)
    {
        return new LSMTree(*this);
    }

    /**
     * @brief Create LSMTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       called SSTable. Each Entry in SpareIndex has range of 1 SSTable
     * @param[in] bufferTreeSize - size of buffered tree
     * @param[in] lvlRatio - nextLvl.size = lvl.size * lvlRatio, lvl0.size = bufferTree.size * lvlRatio
     * @param[in] bulkloadMode - bulkload mode (off, on with current capacity algo, on with max capacity algo)
     *
     * @return LSMTree object
     */
    LSMTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t bufferTreeSize, size_t lvlRatio, enum BulkloadFeatureMode bulkloadMode = BULKLOAD_FEATURE_OFF);

    /**
     * @brief Create LSMTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] bulkloadMode - bulkload mode (off, on with current capacity algo, on with max capacity algo)
     *
     * @return LSMTree object
     */
    LSMTree(Disk* disk, size_t sizeKey, size_t sizeData, enum BulkloadFeatureMode bulkloadMode = BULKLOAD_FEATURE_OFF);

    /**
     * @brief Created brief snapshot of DBIndex as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of DBIndex
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Created full snapshot of DBIndex as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of DBIndex
     */
    virtual std::string toStringFull(bool oneLine = true) const noexcept(true) override;

    /**
     * @brief Get LSMTree Height (number of levels written on disk). When LSM has only BufferTree then height = 0
     *
     * @return LSM Height
     */
    size_t getHeight() const noexcept(true);

    /**
     * @brief Get LSM Lvl const reference to see lvl snapshot
     *
     * @param[in] lvl - LSM LVL, 0 = BufferTree, 1 = first LSMLvl written on disk, LSM Height = last lvl
     *
     * @return const reference to LSMLvl
     */
    const LSMTree::LSMLvl& getLSMLvl(size_t lvl) const noexcept(true);

    /**
     * @brief Check if bulkload operation is supported
     *
     * @return true if bulkload is supported
     *         false otherwise
     */
    virtual bool isBulkloadSupported() const noexcept(true) override;

    /**
     * @brief Insert new entries to the DBIndex
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
     * @brief Insert new entries to the DBIndex by using bulkload
     *        If bulkload is not supported this function does nothing and returns 0.0
     *
     * @param[in] numEntries - entries to insert via bulkload
     *
     * @return time needed to evaluates the operation
     */
    virtual double bulkloadEntries(size_t numEntries = 1) noexcept(true) override;

    /**
     * @brief Delete entries from the DBIndex
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double deleteEntries(size_t numOperations = 1) noexcept(true) override;;

    /**
     * @brief Find entries from the DBIndex using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Find entries from the DBIndex using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(double selectivity, size_t numOperations = 1) noexcept(true) override;;

    /**
     * @brief Find entries from the DBIndex using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] numEntries - number of entries to seek
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(size_t numEntries, size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Find entries from the DBIndex using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(double selectivity, size_t numOperations = 1) noexcept(true) override;

    virtual ~LSMTree() = default;
    LSMTree() = default;
    LSMTree(const LSMTree&) = default;
    LSMTree& operator=(const LSMTree&) = default;
    LSMTree(LSMTree &&) = default;
    LSMTree& operator=(LSMTree &&) = default;
};

#endif