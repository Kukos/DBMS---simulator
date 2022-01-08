#ifndef DB_TABLE_HPP
#define DB_TABLE_HPP

#include <vector>
#include <string>

class DBTable
{
protected:
    const char* name;
    size_t sizeKey;
    size_t sizeData;
    size_t sizeRecord;
    std::vector<size_t> columnSize;
public:

    /**
     * @brief Construct a new DBTable
     *
     * @param[in] columnSize - vector with colum sizes, columnSize[0] - keySize
     */
    DBTable(std::vector<size_t> columnSize);

    /**
     * @brief Construct a new DBTable
     *
     * @param[in] name - table name
     * @param[in] columnSize - vector with colum sizes, columnSize[0] - keySize
     */
    DBTable(const char* name, std::vector<size_t> columnSize);

    /**
     * @brief Virtual constructor idiom
     *
     * @return clone of table
     */
    virtual DBTable* clone() noexcept(true)
    {
        return new DBTable(*this);
    }

    /**
     * @brief Get the Name object
     *
     * @return name
     */
    const char* getName() const noexcept(true)
    {
        return name;
    }

    /**
     * @brief Get the Key Size object
     *
     * @return key size
     */
    size_t getKeySize() const noexcept(true)
    {
        return sizeKey;
    }

    /**
     * @brief Get the Data Size object
     *
     * @return data size
     */
    size_t getDataSize() const noexcept(true)
    {
        return sizeData;
    }

    /**
     * @brief Get the Record Size object
     *
     * @return record size
     */
    size_t getRecordSize() const noexcept(true)
    {
        return sizeRecord;
    }

    /**
     * @brief Get the Column Size object
     *
     * @param[in] column - column index [0] - key
     * @return column size
     */
    size_t getColumnSize(size_t column) const noexcept(true)
    {
        if (column >= columnSize.size())
            return -1;

        return columnSize[column];
    }

    /**
     * @brief Get the Num Column object
     *
     * @return number of columns
     */
    size_t getNumColumns() const noexcept(true)
    {
        return columnSize.size();
    }

    /**
     * @brief Get the All Column Size object
     *
     * @return vector with all column sizes
     */
    const std::vector<size_t>& getAllColumnSize() const noexcept(true)
    {
        return columnSize;
    }

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

    virtual ~DBTable() = default;
    DBTable() = default;
    DBTable(const DBTable&) = default;
    DBTable& operator=(const DBTable&) = default;
    DBTable(DBTable &&) = default;
    DBTable& operator=(DBTable &&) = default;
};

#endif