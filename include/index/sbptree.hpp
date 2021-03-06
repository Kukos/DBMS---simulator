#ifndef SBPTREE_HPP
#define SBPTREE_HPP

#include <index/dbIndex.hpp>

class SBPTree : public DBIndex
{
protected:
    size_t nodeSize;
    size_t sizePtr;
    size_t height;
    bool isSpecialBulkloadFeatureOn;

private:
    size_t calculateNumOfNodes() const noexcept(true);
    size_t calculateNumOfInners() const noexcept(true);
    size_t calculateNumOfLeaves() const noexcept(true);
    size_t calculateNumOfLeavesForEntries(size_t numEntries, size_t entrySize) const noexcept(true);

    size_t calculateHeight() const noexcept(true);

    double findLeaf() noexcept(true);

    double insertEntriesHelper(size_t numOperations) noexcept(true);
    double bulkloadEntriesHelper(size_t numEntries) noexcept(true);
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
        return new SBPTree(*this);
    }

    /**
     * @brief Create SBPTree
     *
     * @param[in] disk -     pointer to disk
     * @param[in] sizeKey -  key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       each node has a pointer into him in a lvl upper
     * @param[in] isSpecialBulkloadFeatureOn - when true enables bulkload on not empty tree, by default false
     * @return SBPTree object
     */
    SBPTree(Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, bool isSpecialBulkloadFeatureOn = false);

    /**
     * @brief Create SBPTree
     *
     * @param[in] name -     dbIndex name
     * @param[in] disk -     pointer to disk
     * @param[in] sizeKey -  key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] nodeSize - node is a virtual contiguous area,
     *                       each node has a pointer into him in a lvl upper
     * @param[in] isSpecialBulkloadFeatureOn - when true enables bulkload on not empty tree, by default false
     * @return SBPTree object
     */
    SBPTree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData, size_t nodeSize, bool isSpecialBulkloadFeatureOn = false);

    /**
     * @brief Create SBPTree
     *
     * @param[in] disk -     pointer to disk
     * @param[in] sizeKey -  key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] isSpecialBulkloadFeatureOn - when true enables bulkload on not empty tree, by default false
     *
     * @return SBPTree object
     */
    SBPTree(Disk* disk, size_t sizeKey, size_t sizeData, bool isSpecialBulkloadFeatureOn = false);

    /**
     * @brief Create SBPTree
     *
     * @param[in] name -     dbIndex name
     * @param[in] disk -     pointer to disk
     * @param[in] sizeKey -  key size in bytes
     * @param[in] sizeData - data size (without key) in bytes
     * @param[in] isSpecialBulkloadFeatureOn - when true enables bulkload on not empty tree, by default false
     *
     * @return SBPTree object
     */
    SBPTree(const char* name, Disk* disk, size_t sizeKey, size_t sizeData, bool isSpecialBulkloadFeatureOn = false);

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
     * @brief Get SBPTree Height
     *
     * @return SBP Height
     */
    size_t getHeight() const noexcept(true);

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

    virtual ~SBPTree() = default;
    SBPTree() = default;
    SBPTree(const SBPTree&) = default;
    SBPTree& operator=(const SBPTree&) = default;
    SBPTree(SBPTree &&) = default;
    SBPTree& operator=(SBPTree &&) = default;
};

#endif