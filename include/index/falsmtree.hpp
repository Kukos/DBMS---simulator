#ifndef FALSMTREE_HPP
#define FALSMTREE_HPP

#include <index/dbIndex.hpp>
#include <vector>

class FALSMTree : public DBIndex
{
public:
    class FALSMLvl
    {
    private:
        size_t sizeInBytes;
        size_t sizeInNodes;
        size_t numEntries;
        size_t numEntriesToDelete;
        size_t maxEntries;
        size_t lvl;
        size_t numOverflowNodes;

        bool isFull() const noexcept(true);
        bool willBeFullAfterMerge(const FALSMLvl& lvl);

        friend class FALSMTree;
    public:
        FALSMLvl(size_t sizeInBytes, size_t recordSize, size_t lvl, size_t nodeSize);

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

        size_t getNumOverFlowNodes() const noexcept(true)
        {
            return numOverflowNodes;
        }

        /**
         * @brief Created brief snapshot of FALSMLvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of FALSMLvl
         */
        std::string toString(bool oneLine = true) const noexcept(true);

        /**
         * @brief Created full snapshot of FALSMLvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of FALSMLvl
         */
        std::string toStringFull(bool oneLine = true) const noexcept(true);

        virtual ~FALSMLvl() = default;
        FALSMLvl() = default;
        FALSMLvl(const FALSMLvl&) = default;
        FALSMLvl& operator=(const FALSMLvl&) = default;
        FALSMLvl(FALSMLvl &&) = default;
        FALSMLvl& operator=(FALSMLvl &&) = default;
    };

protected:
    size_t nodeSize;
    size_t lvlRatio;
    size_t capRatio;

    FALSMLvl bufferTree;
    std::vector<FALSMLvl> levels;

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
    double bulkloadEntriesHelper(size_t numEntries) noexcept(true);

public:
    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndex
    *
    * @return new DBIndex
    */
    DBIndex* clone() const noexcept(true)
    {
        return new FALSMTree(*this);
    }

    /**
     * @brief Create FALSMTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       called SSTable. Each Entry in SpareIndex has range of 1 SSTable
     * @param[in] bufferTreeSize - size of buffered tree
     * @param[in] lvlRatio - nextLvl.size = lvl.size * lvlRatio, lvl0.size = bufferTree.size * lvlRatio
     * @param[in] capRatio - in bulkload we have decition: if (newEnties < lvl.maxEntries / capRatio)
     *
     * @return FALSMTree object
     */
    FALSMTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t bufferTreeSize, size_t lvlRatio, size_t capRatio = 1);

    /**
     * @brief Create FALSMTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     *
     * @return FALSMTree object
     */
    FALSMTree(Disk* disk, size_t sizeKey, size_t sizeData);

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
     * @brief Get FALSMTree Height (number of levels written on disk). When FALSM has only BufferTree then height = 0
     *
     * @return FALSM Height
     */
    size_t getHeight() const noexcept(true);

    /**
     * @brief Get FALSM Lvl const reference to see lvl snapshot
     *
     * @param[in] lvl - FALSM LVL, 0 = BufferTree, 1 = first FALSMLvl written on disk, FALSM Height = last lvl
     *
     * @return const reference to FALSMLvl
     */
    const FALSMTree::FALSMLvl& getFALSMLvl(size_t lvl) const noexcept(true);

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

    virtual ~FALSMTree() = default;
    FALSMTree() = default;
    FALSMTree(const FALSMTree&) = default;
    FALSMTree& operator=(const FALSMTree&) = default;
    FALSMTree(FALSMTree &&) = default;
    FALSMTree& operator=(FALSMTree &&) = default;
};

#endif