#ifndef CFD_TREE_HPP
#define CFD_TREE_HPP

#include <index/dbIndexColumn.hpp>
#include <index/fdtree.hpp>
#include <disk/diskColumnOverlay.hpp>

#include <memory>
#include <vector>

class CFDTree : public DBIndexColumn
{
private:
    std::vector<FDTree> fdColumns;

    std::unique_ptr<DiskColumnOverlay> disk;
    size_t nodeSize;
    size_t headTreeSize;
    size_t lvlRatio;

public:
    /**
     * @brief Create CFDTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] columnsSize - vector with size of each columns [0] - key
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       each node has a pointer into him in a lvl upper
     * @param[in] headTreeSize - size of buffered head tree
     * @param[in] lvlRatio - nextLvl.size = lvl.size * lvlRatio, lvl0.size = headTree.size * lvlRatio
     *
     * @return CFDTree object
     */
    CFDTree(Disk* disk, const std::vector<size_t>& columnsSize, size_t nodeSize, size_t headTreeSize, size_t lvlRatio);

    /**
     * @brief Create CFDTree
     *
     * @param[in] disk - pointer to disk
     * @param[in] columnsSize - vector with size of each columns [0] - key
     *
     * @return CFDTree object
     */
    CFDTree(Disk* disk, const std::vector<size_t>& columnsSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndexColumn
    *
    * @return new DBIndexColumn
    */
    DBIndexColumn* clone() const noexcept(true) override
    {
        return new CFDTree(*this);
    }

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
     * @brief Get Disk as a const reference
     *
     * @return Const reference to disk
     */
    virtual const Disk& getDisk() const noexcept(true) override;

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, double> getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true) override;

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, long> getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true) override;

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true) override;

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true) override;

    /**
     * @brief Reset all counter
     */
    virtual void resetAllCounters() noexcept(true) override;

    /**
     * @brief Get number of entries
     *
     * @return number of entries in DBIndex
     */
    virtual size_t getNumEntries() const noexcept(true) override;

    /**
     * @brief Get CFDTree Height (number of levels written on disk). When CFD has only HeadTree then height = 0
     *
     * @return CFD Height
     */
    size_t getHeight() const noexcept(true);

    /**
     * @brief Get CFD Lvl const reference to see lvl snapshot
     *
     * @param[in] lvl - CFD LVL, 0 = HeadTree, 1 = first CFDLvl written on disk, CFD Height = last lvl
     * @param[in] columnIndex - index of requested column (0 - Key column)
     *
     * @return const reference to FDLvl
     */
    const FDTree::FDLvl& getCFDLvl(size_t lvl, size_t columnIndex) const noexcept(true);

     /**
     * @brief Check if bulkload operation is supported
     *
     * @return true if bulkload is supported
     *         false otherwise
     */
    virtual bool isBulkloadSupported() const noexcept(true) override;

    /**
     * @brief Insert new entries to the DBIndexColumn
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
     * @brief Insert new entries to the DBIndexColumn by using bulkload
     *        If bulkload is not supported this function does nothing and returns 0.0
     *
     * @param[in] numEntries - entries to insert via bulkload
     *
     * @return time needed to evaluates the operation
     */
    virtual double bulkloadEntries(size_t numEntries = 1) noexcept(true) override;

    /**
     * @brief Delete entries from the DBIndexColumn
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
     * @brief Find entries from the DBIndexColumn using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] columnsToFetch - sorted vector with number of columns
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations = 1)  noexcept(true) override;

    /**
     * @brief Find entries from the DBIndex using point search (point seek)
     *        Method get number of operations but evaluates operation one by one
     *        So there is no buffering here.
     *        Cost of operation(3) is always equal to 3x operation(1)
     *
     * @param[in] columnsToFetch - sorted vector with number of columns
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Find entries from the DBIndex using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] columnsToFetch - sorted vector with number of columns
     * @param[in] numEntries - number of entries to seek
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations = 1)  noexcept(true) override;

    /**
     * @brief Find entries from the DBIndex using range search (range seek)
     *        This method finds all contiguous entries using 1 operation (range seek)
     *
     * @param[in] columnsToFetch - sorted vector with number of columns
     * @param[in] selectivity - number from range [0;1]. Find entries = selectivity * index.numEntries
     * @param[in] numOperations - number of operations to evaluate one by one
     *
     * @return time needed to evaluates the operations
     */
    virtual double findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) override;

    /**
     * @brief Fake the insert of numEntries entries to create a topology in fastest way.
     *        This should be used only to test non-empty index in workloads, so this is a total fakeout.
     *        No low-level simulation is done here. We are simulating only insert index code without involving disk simulator.
     *        Counters wouldnt be pegged
     *
     * @param[in] numEntries - how entries to insert to create a topology
     */
    virtual void createTopologyAfterInsert(size_t numEntries = 1) noexcept(true) override;

    virtual ~CFDTree() = default;
    CFDTree() = default;

    CFDTree(const CFDTree&);
    CFDTree& operator=(const CFDTree&);

    CFDTree(CFDTree &&) = default;
    CFDTree& operator=(CFDTree &&) = default;
};


#endif