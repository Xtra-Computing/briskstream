/**
 * @file    hpcOverseer.h
 * @brief	HPC and CPU affinity access library
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */
#ifndef HPCOVERSEER_H_INCLUDED
#define HPCOVERSEER_H_INCLUDED



//////////////
// #INCLUDE //
//////////////
#include <sys/types.h>



/////////////
// #DEFINE //
/////////////

    // Generic:
    #define HPCO_NAME "HPC Overseer version 0.8a"
    #define HPCO_VERSION 0.8

    // Logging:
    #ifdef _DEBUG
        #define HPCO_LOG(...) cout << "[HPCO] " << __VA_ARGS__ << endl
        #define HPCO_LOGERROR(...) cout << "[!] " << __VA_ARGS__ << " [" << __FUNCTION__ << ", line " << __LINE__ << "]" << endl
    #else
        #define HPCO_LOG(...)
        #define HPCO_LOGERROR(...)
    #endif

    // Limits:
    #define HPCO_MAXLINE 256



///////////////////////
// CLASS HpcOverseer //
///////////////////////

class HpcOverseer
{
//////////
public: //
//////////

    // Init/free:
    static bool initialize();
    static bool terminate();

    // Get/set:
    static const int getVersion();
    static const int getNumberOfNumas();
    static const int getNumberOfCpusPerNuma();
    static const int getNumberOfCores();
    static const int getNumberOfCpus();
    static const int getNumberOfCoresPerCpu();
    static const int getNumberOfPus();
    static const int getNumberOfThreads();
    static const int getNumberOfInitializedEvents();
    static const int getNumberOfAvailableEvents();
    static const int getCpuCoreMapping(int cpu, int core);
    static const char *getAvailableEventsString();
    static const char *getInitializedEventString(int event);

    // Event manager:
    static bool initEvents(const char *event);
    static bool bindEventsToCore(int coreId);
    static bool bindEventsToThread(pid_t pId);
    static bool start();
    static bool stop();

    // Thread/core events:
    static int getNumberOfEventsFromCore(pid_t pId);
    static int getNumberOfEventsFromThread(pid_t pId);
    static char *getEventNameFromCore(pid_t pId, int event);
    static char *getEventNameFromThread(pid_t pId, int event);
    static long getEventFromCore(int coreId, int event);
    static long getEventFromThread(pid_t pId, int event);

    // Tools:
    static pid_t getThreadId();


///////////
private: //
///////////

    // Testing:
    static bool parseCpuInfo();
    static bool parseHwlocInfo();

    // Flags:
    static bool initialized;

    // Config:
    static int numberOfNumas;
    static int numberOfCpusPerNuma;
    static int numberOfCores;
    static int numberOfCpus;
    static int numberOfCoresPerCpu;
    static int numberOfPus;
    static int numberOfThreads;
    static int numberOfEvents;
    static int numberOfAvailableEvents;
    static int **cpuCoreMapping;
    static char *availableEvent;

    // Reserved:
    static void *_reserved;
};



///////////////////////////
// CLASS RdtscTimerEntry //
///////////////////////////

class RdtscTimerEntry
{
//////////
public: //
//////////

    // Cleaning:
    static void reset();

    // Get/set:
    static int getNumberOfEntries();
    static void setEntry(int paramA, int paramB, int paramC);

    // Logging:
    static bool logToFile(const char *filename);


///////////
private: //
///////////

    // Const/dest:
    RdtscTimerEntry();
    ~RdtscTimerEntry();

    // Params:
    int paramA, paramB, paramC;

    // RDTSC:
    unsigned long long int rdtsc;

    // Chained list:
    RdtscTimerEntry *prev, *next;

    // Static:
    static RdtscTimerEntry *firstEntry;
    static RdtscTimerEntry *lastEntry;
};



////////////////////////
// SCHEDULING METHODS //
////////////////////////

bool HPCO_SetThreadAffinity(pid_t pId, unsigned long mask);
bool HPCO_SetThreadAffinityMask(pid_t pId, int *maskArray, int size);
long HPCO_GetThreadAffinity(pid_t pId);



////////////////////////////////
#endif // HPCOVERSEER_H_INCLUDED
