#include <stdlib.h>
#include <iostream>
#include "hpcOverseer.h"
using namespace std;



// Vars:
int dummythreadpid = 0;



// Thread dummy:
void *dummyThread(void *)
{
    dummythreadpid = HpcOverseer::getThreadId();
    cout << "Dummy thread started (pid: " << dummythreadpid << ")..." << endl;


    int c=0;
    while (c<5)
    {
        sleep(1);
        c++;
    }

    dummythreadpid = 0;
    cout << "Dummy thread finished..." << endl;

    return 0;
}



// Main:
int main(int argc, char *argv[])
{
    cout << HPCO_NAME << " Multi-threaded tester - A. Peternier (C) 2010 USI" << endl << endl;

    // Init events:
    if (HpcOverseer::initialize() == false)
    {
        cout << "Unable to init HpcOverseer" << endl;
        return 1;
    }

    // Credit version:
    cout << "   Version: " << (HpcOverseer::getVersion() / 10.0) << endl;

    if (HpcOverseer::initEvents("PERF_COUNT_HW_CACHE_MISSES:u=1, PERF_COUNT_HW_CPU_CYCLES:u=1, PERF_COUNT_HW_INSTRUCTIONS:u=1") == false)
    {
        cout << "Unable to init selected events" << endl;
        return 2;
    }
    cout << "   " << HpcOverseer::getNumberOfInitializedEvents() << " event(s) initialized:" << endl;
    for (int c=0; c<HpcOverseer::getNumberOfInitializedEvents(); c++)
        cout << "   " << c << ". " << HpcOverseer::getInitializedEventString(c) << endl;
    pid_t pid = HpcOverseer::getThreadId();
    cout << "   Main thread PID: " << pid << endl << endl;

    // Attach events to main thread:
    pthread_t tid1;
    if (pthread_create(&tid1, NULL, dummyThread, NULL))
        cout << "Error creating thread" << endl;

    // Wait for the other thread to start:
    while (dummythreadpid == 0)
    {
        sleep(1);
    }

    if (HpcOverseer::bindEventsToThread(dummythreadpid) == false)
    {
        cout << "Unable to bind events to dummy thread" << endl;
        return 3;
    }

    // Read some events:
    cout << "Reading events from dummy thread (ctrl+C to abort)..." << endl;
    while (dummythreadpid)
    {
        // Display some info:
        cout << endl << "   From thread: " << dummythreadpid << endl;
        long v = HpcOverseer::getEventFromThread(dummythreadpid, 0);
        cout << "      cache misses: " << v << endl;
        v = HpcOverseer::getEventFromThread(dummythreadpid, 1);
        cout << "      cycles: " << v << endl;
        v = HpcOverseer::getEventFromThread(dummythreadpid, 2);
        cout << "      instructions: " << v << endl;

        sleep(1);
    }

    // Shutdown:
    if (HpcOverseer::terminate() == false)
    {
        cout << "Unable to shutdown HpcOverseer" << endl;
        return 5;
    }
    cout << endl << "Application terminated" << endl;
    return 0;
}
