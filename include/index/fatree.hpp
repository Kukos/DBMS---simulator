#ifndef FATREE_HPP
#define FATREE_HPP

#include <index/dbIndex.hpp>
#include <vector>

class FATree : public DBIndex
{
public:
    class FALvl
    {
    private:
        size_t sizeInBytes;
        size_t numEntries;
        size_t numEntriesToDelete;
        size_t maxEntries;
        size_t lvl;

        bool isFull() const noexcept(true);
        bool willBeFullAfterMerge(const FALvl& lvl);

        friend class FATree;
    public:
        FALvl(size_t sizeInBytes, size_t recordSize, size_t lvl);

        size_t getSize() const noexcept(true)
        {
            return sizeInBytes;
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
         * @brief Created brief snapshot of FALvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return brief edscription of FALvl
         */
        std::string toString(bool oneLine = true) const noexcept(true);

        /**
         * @brief Created full snapshot of FALvl as a string
         *
         * @param[in] oneLine - create string as 1 line or not? By default Yes
         * @return Full edscription of FALvl
         */
        std::string toStringFull(bool oneLine = true) const noexcept(true);

        virtual ~FALvl() = default;
        FALvl() = default;
        FALvl(const FALvl&) = default;
        FALvl& operator=(const FALvl&) = default;
        FALvl(FALvl &&) = default;
        FALvl& operator=(FALvl &&) = default;
    };

protected:
    size_t nodeSize;
    size_t lvlRatio;

    FALvl headTree;
    std::vector<FALvl> levels;

private:
    double insertIntoHeadTree(size_t entries) noexcept(true);
    double deleteFromHeadTree(size_t entries) noexcept(true);
    void addLevel() noexcept(true);

    double mergeHeadTree() noexcept(true);
    double mergeHeadTreeFake() noexcept(true);
    double mergeLevels(size_t upperLvl, size_t lowerLvl) noexcept(true);
    double mergeLevelsFake(size_t upperLvl, size_t lowerLvl) noexcept(true);

    double insertEntriesHelper(size_t numOperations) noexcept(true);
    double deleteEntriesHelper(size_t numOperations) noexcept(true);
    double findEntriesHelper(size_t numEntries, size_t numOperations) noexcept(true);

public:
    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndex
    *
    * @return new DBIndex
    */
    DBIndex* clone() const noexcept(true)
    {
        return new FATree(*this);
    }


    /**
     * @brief Create FATree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       each node has a pointer into him in a lvl upper
     * @param[in] headTreeSize - size of buffered head tree
     * @param[in] lvlRatio - nextLvl.size = lvl.size * lvlRatio, lvl0.size = headTree.size * lvlRatio
     *
     * @return FATree object
     */
    FATree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t headTreeSize, size_t lvlRatio);

    /**
     * @brief Create FATree
     *
     * @param[in] name -     dbIndex name
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       each node has a pointer into him in a lvl upper
     * @param[in] headTreeSize - size of buffered head tree
     * @param[in] lvlRatio - nextLvl.size = lvl.size * lvlRatio, lvl0.size = headTree.size * lvlRatio
     *
     * @return FATree object
     */
    FATree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, size_t headTreeSize, size_t lvlRatio);

    /**
     * @brief Create FATree
     *
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     *
     * @return FATree object
     */
    FATree(Disk* disk, size_t sizeKey, size_t sizeData);

    /**
     * @brief Create FATree
     *
     * @param[in] name -     dbIndex name
     * @param[in] disk - pointer to disk
     * @param[in] sizeKey - key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     *
     * @return FATree object
     */
    FATree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData);

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
     * @brief Get FATree Height (number of levels written on disk). When FA has only HeadTree then height = 0
     *
     * @return FA Height
     */
    size_t getHeight() const noexcept(true);

    /**
     * @brief Get FA Lvl const reference to see lvl snapshot
     *
     * @param[in] lvl - FA LVL, 0 = HeadTree, 1 = first FALvl written on disk, FA Height = last lvl
     *
     * @return const reference to FALvl
     */
    const FATree::FALvl& getFALvl(size_t lvl) const noexcept(true);

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

    /**
     * @brief Fake the insert of numEntries entries to create a topology in fastest way.
     *        This should be used only to test non-empty index in workloads, so this is a total fakeout.
     *        No low-level simulation is done here. We are simulating only insert index code without involving disk simulator.
     *        Counters wouldnt be pegged
     *
     * @param[in] numEntries - how entries to insert to create a topology
     */
    virtual void createTopologyAfterInsert(size_t numEntries = 1) noexcept(true) override;

    virtual ~FATree() = default;
    FATree() = default;
    FATree(const FATree&) = default;
    FATree& operator=(const FATree&) = default;
    FATree(FATree &&) = default;
    FATree& operator=(FATree &&) = default;
};

#endif