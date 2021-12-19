#ifndef DB_INDEX_RAW_TO_COLUMN_WRAPPER_HPP
#define DB_INDEX_RAW_TO_COLUMN_WRAPPER_HPP

#include <index/dbIndex.hpp>
#include <index/dbIndexColumn.hpp>

#include <memory>

class DBIndexRawToColumnWrapper : public DBIndexColumn
{
private:
    std::unique_ptr<DBIndex> rawIndex;
public:
    /**
     * @brief Create DBIndexRawToColumnWrapper
     *
     * @param[in] disk - pointer to disk
     * @param[in] columnsSize - vector with size of each columns [0] - key
     *
     * @return DBIndexRawToColumnWrapper object
     */
    DBIndexRawToColumnWrapper(DBIndex* rawIndex, const std::vector<size_t>& columnsSize);

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
     * @brief Get Disk as a const reference
     *
     * @return Const reference to disk
     */
    virtual const Disk& getDisk() const noexcept(true) override
    {
        return rawIndex->getDisk();
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, double> getCounter(enum IndexCounters::IndexCountersD counterId) const noexcept(true) override
    {
        return rawIndex->getCounter(counterId);
    }

    /**
     * @brief Get Counter as a pair
     *
     * @param[in] counterId - Counter ID
     *
     * @return Counter pair with name and value
     */
    virtual std::pair<std::string, long> getCounter(enum IndexCounters::IndexCountersL counterId) const noexcept(true) override
    {
        return rawIndex->getCounter(counterId);
    }

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersD counterId) noexcept(true) override
    {
        rawIndex->resetCounter(counterId);
    }

    /**
     * @brief Reset counter
     *
     * @param[in] counterId - Counter ID
     */
    virtual void resetCounter(enum IndexCounters::IndexCountersL counterId) noexcept(true) override
    {
        rawIndex->resetCounter(counterId);
    }

    /**
     * @brief Reset all counter
     */
    virtual void resetAllCounters() noexcept(true) override
    {
        rawIndex->resetAllCounters();
    }

    /**
     * @brief Get number of entries
     *
     * @return number of entries in DBIndexRawToColumnWrapper
     */
    virtual size_t getNumEntries() const noexcept(true) override;

    /**
    * @brief Virtual constructor idiom implemented as clone function. This function creates new DBIndexColumn
    *
    * @return new DBIndexColumn
    */
    DBIndexColumn* clone() const noexcept(true) override
    {
        return new DBIndexRawToColumnWrapper(*this);
    }

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

    virtual ~DBIndexRawToColumnWrapper() = default;
    DBIndexRawToColumnWrapper() = default;

    DBIndexRawToColumnWrapper(const DBIndexRawToColumnWrapper&);
    DBIndexRawToColumnWrapper& operator=(const DBIndexRawToColumnWrapper&);

    DBIndexRawToColumnWrapper(DBIndexRawToColumnWrapper &&) = default;
    DBIndexRawToColumnWrapper& operator=(DBIndexRawToColumnWrapper &&) = default;
};

#endif