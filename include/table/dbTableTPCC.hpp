#ifndef DB_TABLE_TPC_C_HPP
#define DB_TABLE_TPC_C_HPP

#include <table/dbTable.hpp>

class DBTable_TPCC_Warehouse : public DBTable
{
public:
    DBTable_TPCC_Warehouse()
    : DBTable("TPC-C Warehouse", std::vector<size_t>{8, 10, 20, 20, 20, 2, 9, 8, 16})
    {

    }

    /**
     * @brief Virtual constructor idiom
     *
     * @return clone of table
     */
    virtual DBTable* clone() noexcept(true)
    {
        return new DBTable_TPCC_Warehouse(*this);
    }

    virtual ~DBTable_TPCC_Warehouse() = default;
    DBTable_TPCC_Warehouse(const DBTable_TPCC_Warehouse&) = default;
    DBTable_TPCC_Warehouse& operator=(const DBTable_TPCC_Warehouse&) = default;
    DBTable_TPCC_Warehouse(DBTable_TPCC_Warehouse &&) = default;
    DBTable_TPCC_Warehouse& operator=(DBTable_TPCC_Warehouse &&) = default;
};

class DBTable_TPCC_Customer : public DBTable
{
public:
    DBTable_TPCC_Customer()
    : DBTable("TPC-C Customer", std::vector<size_t>{8, 8, 8, 16, 2, 16, 20, 20, 20, 2, 9, 16, 8, 2, 16, 8, 16, 16, 4, 4, 500})
    {

    }

    /**
     * @brief Virtual constructor idiom
     *
     * @return clone of table
     */
    virtual DBTable* clone() noexcept(true)
    {
        return new DBTable_TPCC_Customer(*this);
    }

    virtual ~DBTable_TPCC_Customer() = default;
    DBTable_TPCC_Customer(const DBTable_TPCC_Customer&) = default;
    DBTable_TPCC_Customer& operator=(const DBTable_TPCC_Customer&) = default;
    DBTable_TPCC_Customer(DBTable_TPCC_Customer &&) = default;
    DBTable_TPCC_Customer& operator=(DBTable_TPCC_Customer &&) = default;
};

class DBTable_TPCC_Stock : public DBTable
{
public:
    DBTable_TPCC_Stock()
    : DBTable("TPC-C Stock", std::vector<size_t>{8, 8, 4, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 8, 4, 4, 50})
    {

    }

    /**
     * @brief Virtual constructor idiom
     *
     * @return clone of table
     */
    virtual DBTable* clone() noexcept(true)
    {
        return new DBTable_TPCC_Stock(*this);
    }

    virtual ~DBTable_TPCC_Stock() = default;
    DBTable_TPCC_Stock(const DBTable_TPCC_Stock&) = default;
    DBTable_TPCC_Stock& operator=(const DBTable_TPCC_Stock&) = default;
    DBTable_TPCC_Stock(DBTable_TPCC_Stock &&) = default;
    DBTable_TPCC_Stock& operator=(DBTable_TPCC_Stock &&) = default;
};

#endif