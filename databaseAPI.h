#include <sstream>
#include <vector>
#include <thread>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <cstdint>
#include <inttypes.h>
#include <string.h>
#include <mysql.h>
#include "binaryblob.h"
using namespace std;

#define BILLION 1000000000L
#define RECORDS_PER_THREAD (3251041/12)
//#define RECORDS_PER_THREAD 1

class FFError
{
public:
    std::string Label;

    FFError() { Label = (char *)"Generic Error"; }
    FFError(char *message) { Label = message; }
    ~FFError() {}
    inline const char *GetMessage(void) { return Label.c_str(); }
};

void add_session(MYSQL *Conn, std::string &cacheId, uint64_t &resourceSize, uint64_t &resourceCreated, string &resourceHeader, string &resourceUrl)
{
    void *bin_data = rawDataOne;
    size_t bin_size = 4425; // -- ...and the size of the binary data

    try
    {
        char chunk[2 * bin_size + 1];
        mysql_real_escape_string(Conn, chunk, rawDataOne, bin_size);

        char *st = "INSERT INTO cachedbtable (cache_id, resource_size, resource_created, resource_headers, resource_url, resource_data) VALUES ('%s',%lld,%lld,'%s','%s','%s');";
        size_t st_len = strlen(st);

        int totalSize = (st_len + (2 * bin_size) + (19 * 2) + cacheId.size() + resourceHeader.size() + resourceUrl.size());
        char query[totalSize + 1];
        int len = snprintf(query, (totalSize + 1), st, cacheId.c_str(), resourceSize, resourceCreated, resourceHeader.c_str(), resourceUrl.c_str(), chunk);

        //printf("\nquery : [%s] len [%d]\n", query, strlen(query));
        int mysqlStatus = mysql_real_query(Conn, query, totalSize + 1);
        if (mysqlStatus)
        {
            throw FFError((char *)mysql_error(Conn));
        }
    }
    catch (FFError e)
    {
        printf("\n1 ERROR while inserting data to db : %s\n", e.Label.c_str());
    }
    catch (const std::exception &e)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << e.what() << std::endl;
    }
    catch (...)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
    }
}

void generateRandomData(string &cacheId, uint64_t &resourceSize, uint64_t &resourceCreated, string &resourceHeader, string &resourceUrl, int &arg)
{
    long int ns;
    time_t sec;
    struct timespec spec;

    clock_gettime(CLOCK_REALTIME, &spec);
    sec = spec.tv_sec;
    ns = spec.tv_nsec;

    uint64_t randomNum = (uint64_t)sec * BILLION + (uint64_t)ns;

    cacheId = "";
    std::ostringstream oss;
    oss << randomNum;
    oss << "_" << arg;
    cacheId.append(oss.str());

    resourceSize = sec;
    resourceCreated = sec;
    resourceHeader = cacheId;
    resourceUrl = cacheId;
}

void workerThreadInsert(int arg)
{
    MYSQL *MySQLConRet;
    MYSQL *MySQLConnection = NULL;

    string hostName = "localhost";
    string userId = "root";
    string password = "Google@123";
    string DB = "cachedatabase";

    MySQLConnection = mysql_init(NULL);

    try
    {
        MySQLConRet = mysql_real_connect(MySQLConnection, hostName.c_str(), userId.c_str(), password.c_str(), DB.c_str(), 0, NULL, 0);

        if (MySQLConRet == NULL)
        {
            throw FFError((char *)mysql_error(MySQLConnection));
        }
    }
    catch (FFError e)
    {
        printf("\n2. ERROR while inserting data to db : %s\n", e.Label.c_str());
    }

    time_t startTime = time(0);
    printf("writter thread [%d] started at [%d] for [%d] entries\n", arg, startTime, RECORDS_PER_THREAD);
    try
    {
        int count = 0;
        while (count < RECORDS_PER_THREAD)
        {

            std::string cacheId;
            uint64_t resourceSize;
            uint64_t resourceCreated;
            string resourceHeader;
            string resourceUrl;

            generateRandomData(cacheId, resourceSize, resourceCreated, resourceHeader, resourceUrl, arg);
            add_session(MySQLConnection, cacheId, resourceSize, resourceCreated, resourceHeader, resourceUrl);

            /*sleep for 1/10 (100000 us) of second*/
            //usleep(100000);
            count++;
        }
        time_t endTime = time(0);
        printf("writter thread [%d] stopped at [%d] after inserting [%d] rows, time taken = [%d] seconds\n", arg, endTime, RECORDS_PER_THREAD, (endTime - startTime));
    }
    catch (FFError e)
    {
        printf("\n3. ERROR while inserting data to db : %s\n", e.Label.c_str());
    }
    catch (const std::exception &e)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << e.what() << std::endl;
        std::cerr << "thread number : " << arg << std::endl;
    }
    catch (...)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << "thread number : " << arg << std::endl;
    }
    mysql_thread_end();
}

void workerThreadRead(int arg)
{
#if 1
    MYSQL *MySQLConRet;
    MYSQL *MySQLConnection = NULL;

    string hostName = "localhost";
    string userId = "root";
    string password = "Google@123";
    string DB = "cachedatabase";

    MySQLConnection = mysql_init(NULL);

    try
    {
        MySQLConRet = mysql_real_connect(MySQLConnection, hostName.c_str(), userId.c_str(), password.c_str(), DB.c_str(), 0, NULL, 0);

        if (MySQLConRet == NULL)
        {
            throw FFError((char *)mysql_error(MySQLConnection));
        }
    }
    catch (FFError e)
    {
        printf("\n1. ERROR while reading data from db : %s\n", e.Label.c_str());
    }

    time_t startTime = time(0);
    printf("reader thread [%d] started at [%d] for [%d] entries\n", arg, startTime, RECORDS_PER_THREAD);
    try
    {
        int mysqlStatus = 0;
        MYSQL_RES *mysqlResult = NULL;
        int count = 0;
        while (count < RECORDS_PER_THREAD)
        {
            std::string sessionId;
            int pageSeq = 0;
            std::string cacheId;
            std::string eventId;

            MYSQL_ROW mysqlRow;
            MYSQL_FIELD *mysqlFields;
            my_ulonglong numRows;
            unsigned int numFields;

            string sqlSelStatement = "SELECT * FROM cachedbtable ORDER BY cache_id DESC limit 1";
            mysqlStatus = mysql_query(MySQLConnection, sqlSelStatement.c_str());

            if (mysqlStatus)
            {
                throw FFError((char *)mysql_error(MySQLConnection));
            }
            else
            {
                mysqlResult = mysql_store_result(MySQLConnection); // Get the Result Set
            }

            if (mysqlResult) // there are rows
            {
                // # of rows in the result set
                numRows = mysql_num_rows(mysqlResult);

                // # of Columns (mFields) in the latest results set
                numFields = mysql_field_count(MySQLConnection);

                // Returns the number of columns in a result set specified
                numFields = mysql_num_fields(mysqlResult);

                //printf("Number of rows=%u  Number of fields=%u \n", numRows, numFields);
            }
            else
            {
                printf("Result set is empty");
            }

            if (mysqlResult)
            {
                mysql_free_result(mysqlResult);
                mysqlResult = NULL;
            }

            /*sleep for 1/10 (100000 us) of second*/
            //usleep(10000);
            count++;
        }
        time_t endTime = time(0);
        printf("reader thread [%d] stopped at [%d] after %d times read operation, time taken = [%d] seconds\n", arg, endTime, count, (endTime - startTime));
    }
    catch (FFError e)
    {
        printf("\n1 ERROR while inserting data to db : %s\n", e.Label.c_str());
    }
    catch (const std::exception &e)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << e.what() << std::endl;
        std::cerr << "thread number : " << arg << std::endl;
    }
    catch (...)
    {
        std::cout << "ERRRORRRRRRRRRRRRR" << endl;
        std::cerr << "thread number : " << arg << std::endl;
    }
#endif
}