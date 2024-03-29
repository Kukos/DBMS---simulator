#include <iostream>

#include <logger/logger.hpp>
#include <threadPool/dbThreadPool.hpp>

extern void experimentLSMPaper();
extern void sandboxLSMPaper();
extern void experimentPhdFATree();
extern void experimentPhdFALSMTree();
extern void experimentPhdCFDTree();
extern void experimentPhdLAM();
extern void experimentPhdPAM();


int main()
{
    loggerStart();
    loggerSetLevel(static_cast<enum logger_levels>(LOGGER_ACTIVE_LEVEL));

    std::cout << "Database Management System - simulator" << std::endl;

    // sandboxLSMPaper();
    // experimentLSMPaper();

    // experimentPhdFATree();
    // experimentPhdFALSMTree();
    // experimentPhdCFDTree();
    // experimentPhdLAM();
    experimentPhdPAM();


    DBThreadPool::threadPool.wait_for_tasks();

    return 0;
}
