#ifndef DB_THREAD_POOL_HPP
#define DB_THREAD_POOL_HPP

#include <threadPool/threadPool.hpp>

#include <thread>

class DBThreadPool
{
public:
    static thread_pool threadPool;
    static std::mutex mutex;
};

#endif