#ifndef DSM_HPP
#define DSM_HPP

#include <index/dbIndexColumn.hpp>

class FBDSM : public DBIndexColumn
{
protected:
    static constexpr size_t insertBufferMaxSize = 4000;
    size_t entriesInInsertBuffer;
private:
    double findKey() noexcept(true);
    double insertEntriesHelper(size_t numOperations) noexcept(true);
    double bulkloadEntriesHelper(size_t numEntries) noexcept(true);
    double deleteEntriesHelper(size_t numOperations) noexcept(true);
    double findEntriesHelper(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations) noexcept(true);
public:
    /**
     * @brief Create DSM
     *
     * @param[in] disk - pointer to disk
     * @param[in] columnsSize - vector with size of each columns [0] - key
     *
     * @return DSM object
     */
    FBDSM(Disk* disk, const std::vector<size_t>& columnsSize);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndexColumn
    *
    * @return new DBIndexColumn
    */
    DBIndexColumn* clone() const noexcept(true) override
    {
        return new FBDSM(*this);
    }

    /**
     * @brief Get number of entries
     *
     * @return number of entries in DBIndex
     */
    virtual size_t getNumEntries() const noexcept(true) override;

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

    virtual ~FBDSM() = default;
    FBDSM() = default;
    FBDSM(const FBDSM&) = default;
    FBDSM& operator=(const FBDSM&) = default;
    FBDSM(FBDSM &&) = default;
    FBDSM& operator=(FBDSM &&) = default;
};

#endif