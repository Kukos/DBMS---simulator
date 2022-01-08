#include <table/dbTable.hpp>
#include <string>
#include <iostream>

#include <gtest/gtest.h>

GTEST_TEST(dbTableTest, interface)
{
    std::vector<size_t> cols {1, 2, 3, 4, 5, 6};
    std::string name("test");
    DBTable table(name.c_str(), cols);

    EXPECT_EQ(table.getName(), name.c_str());
    EXPECT_EQ(table.getKeySize(), 1);
    EXPECT_EQ(table.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getAllColumnSize(), cols);

    EXPECT_EQ(table.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(table.getColumnSize(i), cols[i]);
}

GTEST_TEST(dbTableTest, copy)
{
    std::vector<size_t> cols {1, 2, 3, 4, 5, 6};
    std::string name("test");
    DBTable table(name.c_str(), cols);

    EXPECT_EQ(table.getName(), name.c_str());
    EXPECT_EQ(table.getKeySize(), 1);
    EXPECT_EQ(table.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getAllColumnSize(), cols);

    EXPECT_EQ(table.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(table.getColumnSize(i), cols[i]);

    DBTable copy(table);
    EXPECT_EQ(copy.getName(), name.c_str());
    EXPECT_EQ(copy.getKeySize(), 1);
    EXPECT_EQ(copy.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy.getAllColumnSize(), cols);

    EXPECT_EQ(copy.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(copy.getColumnSize(i), cols[i]);

    DBTable copy2;
    copy2 = copy;
    EXPECT_EQ(copy2.getName(), name.c_str());
    EXPECT_EQ(copy2.getKeySize(), 1);
    EXPECT_EQ(copy2.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy2.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy2.getAllColumnSize(), cols);

    EXPECT_EQ(copy2.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), cols[i]);
}

GTEST_TEST(dbTableTest, move)
{
    std::vector<size_t> cols {1, 2, 3, 4, 5, 6};
    std::string name("test");
    DBTable table(name.c_str(), cols);

    EXPECT_EQ(table.getName(), name.c_str());
    EXPECT_EQ(table.getKeySize(), 1);
    EXPECT_EQ(table.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(table.getAllColumnSize(), cols);

    EXPECT_EQ(table.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(table.getColumnSize(i), cols[i]);

    DBTable copy(std::move(table));
    EXPECT_EQ(copy.getName(), name.c_str());
    EXPECT_EQ(copy.getKeySize(), 1);
    EXPECT_EQ(copy.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy.getAllColumnSize(), cols);

    EXPECT_EQ(copy.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(copy.getColumnSize(i), cols[i]);

    DBTable copy2;
    copy2 = std::move(copy);
    EXPECT_EQ(copy2.getName(), name.c_str());
    EXPECT_EQ(copy2.getKeySize(), 1);
    EXPECT_EQ(copy2.getDataSize(), 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy2.getRecordSize(), 1 + 2 + 3 + 4 + 5 + 6);
    EXPECT_EQ(copy2.getAllColumnSize(), cols);

    EXPECT_EQ(copy2.getNumColumns(), cols.size());
    for (size_t i = 0; i < cols.size(); ++i)
        EXPECT_EQ(copy2.getColumnSize(i), cols[i]);
}