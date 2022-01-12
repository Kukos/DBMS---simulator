#ifndef DB_INDEX_COLUMN_HPP
#define DB_INDEX_COLUMN_HPP

#include <memory>
#include <cstddef>
#include <vector>
#include <disk/disk.hpp>
#include <observability/indexCounters.hpp>

class DBIndexColumn
{
protected:
    const char* name;
    IndexCounters counters;
    std::unique_ptr<Disk> disk;
    std::vector<size_t> columnsSize;
    size_t sizeKey; // columnsSize[0]
    size_t sizeData;  // columnsSize[1 ... n]
    size_t sizeRecord; // columnsSize[0 ... n]
    size_t numEntries;

public:
    /**
     * @brief Create DBIndexColumn
     *
     * @param[in] name - Index name
     * @param[in] disk - pointer to disk
     * @param[in] columnsSize - vector with size of each columns [0] - key
     *
     * @return DBIndex object
     */
    DBIndexColumn(const char*name, Disk* disk, const std::vector<size_t>& columnsSize);

    /**
     * @brief Reset non-const values to default value
     *
     */
    virtual void resetState() noexcept(true);

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndexColumn
    *
    * @return new DBIndexColumn
    */
    virtual DBIndexColumn* clone() const noexcept(true) = 0;

    /**
     * @brief Created brief snapshot of DBIndex as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return brief edscription of DBIndex
     */
    virtual std::string toString(bool oneLine = true) const noexcept(true);

    /**
     * @brief Created full snapshot of DBIndex as a string
     *
     * @param[in] oneLine - create string as 1 line or not? By default Yes
     * @return Full edscription of DBIndex
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
        return *disk.get();
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, double> getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true)
    {
        return counters.getCounter(counterId);
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, long> getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true)
    {
        return counters.getCounter(counterId);
    }

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true)
    {
        counters.resetCounter(counterId);
    }

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true)
    {
        counters.resetCounter(counterId);
    }

    /**
     * @brief Reset all counter
     */
    virtual void resetAllCounters() noexcept(true)
    {
        counters.resetAllCounters();
    }

    /**
     * @brief Get number of entries
     *
     * @return number of entries in DBIndex
     */
    virtual size_t getNumEntries() const noexcept(true)
    {
        return numEntries;
    }

    /**
     * @brief Get size of key
     *
     * @return size of key
     */
    virtual size_t getKeySize() const noexcept(true)
    {
        return sizeKey;
    }

    /**
     * @brief Get size of data
     *
     * @return size of data
     */
    virtual size_t getDataSize() const noexcept(true)
    {
        return sizeData;
    }

    /**
     * @brief get size of whole record
     *
     * @return size of record
     */
    virtual size_t getRecordSize() const noexcept(true)
    {
        return sizeRecord;
    }

    /**
     * @brief get number of columns
     *
     * @return number of columns
     */
    virtual size_t getNumOfColumns() const noexcept(true)
    {
        return columnsSize.size();
    }

    /**
     * @brief get size of selected column
     *
     * @param[in] columnIndex - index of selected column
     *
     * @return size of selected column
     */
    virtual size_t getColumnSize(size_t columnIndex) const noexcept(true)
    {
        if (columnIndex < columnsSize.size())
            return columnsSize[columnIndex];

        return 0;
    }

     /**
     * @brief Check if bulkload operation is supported
     *
     * @return true if bulkload is supported
     *         false otherwise
     */
    virtual bool isBulkloadSupported() const noexcept(true)
    {
        // by default buylkload is unsupported
        return false;
    }

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
    virtual double insertEntries(size_t numOperations = 1) noexcept(true) = 0;

    /**
     * @brief Insert new entries to the DBIndexColumn by using bulkload
     *        If bulkload is not supported this function does nothing and returns 0.0
     *
     * @param[in] numEntries - entries to insert via bulkload
     *
     * @return time needed to evaluates the operation
     */
    virtual double bulkloadEntries(size_t numEntries = 1) noexcept(true) = 0;

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
    virtual double deleteEntries(size_t numOperations = 1) noexcept(true) = 0;

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
    virtual double findPointEntries(const std::vector<size_t>& columnsToFetch, size_t numOperations = 1)  noexcept(true) = 0;

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
    virtual double findPointEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) = 0;

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
    virtual double findRangeEntries(const std::vector<size_t>& columnsToFetch, size_t numEntries, size_t numOperations = 1)  noexcept(true) = 0;

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
    virtual double findRangeEntries(const std::vector<size_t>& columnsToFetch, double selectivity, size_t numOperations = 1) noexcept(true) = 0;

    /**
     * @brief Fake the insert of numEntries entries to create a topology in fastest way.
     *        This should be used only to test non-empty index in workloads, so this is a total fakeout.
     *        No low-level simulation is done here. We are simulating only insert index code without involving disk simulator.
     *        Counters wouldnt be pegged
     *
     * @param[in] numEntries - how entries to insert to create a topology
     */
    virtual void createTopologyAfterInsert(size_t numEntries = 1) noexcept(true) = 0;

    virtual ~DBIndexColumn() = default;
    DBIndexColumn() = default;

    DBIndexColumn(const DBIndexColumn&);
    DBIndexColumn& operator=(const DBIndexColumn&);

    DBIndexColumn(DBIndexColumn &&) = default;
    DBIndexColumn& operator=(DBIndexColumn &&) = default;
};

#endif