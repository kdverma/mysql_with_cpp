//#define _GLIBCXX_USE_CXX11_ABI 0
#include <iostream>
#include <thread>
#include "databaseAPI.h"

#define NUM_OF_THREAD 12
int main()
{

    if (mysql_library_init(0, NULL, NULL))
    {
        fprintf(stderr, "could not initialize MySQL client library\n");
        exit(1);
    }
    try
    {
        long id;
        std::vector<std::thread> writterThreads;
        std::vector<std::thread> readerThreads;
        for (id = 1; id <= NUM_OF_THREAD; id++)
        {
            writterThreads.emplace_back(std::thread(workerThreadInsert, id));
        }

        for (id = 1; id <= NUM_OF_THREAD; id++)
        {
            readerThreads.emplace_back(std::thread(workerThreadRead, id));
        }

        printf("Waiting for thread to finish...\n");

        for (auto &t : writterThreads)
        {
            if (t.joinable())
            {
                t.join();
            }
        }

        for (auto &t : readerThreads)
        {
            if (t.joinable())
            {
                t.join();
            }
        }
    }
    catch (FFError e)
    {
        printf("\nERROR : %s\n", e.Label.c_str());
        //return 1;
    }
    catch (const std::exception &e)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << e.what() << std::endl;
    }
    mysql_library_end();
    return 0;
}