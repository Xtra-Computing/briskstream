#include <stdlib.h>
#include <iostream>
#include "hpcOverseer.h"


#define TESTTIME 5
using namespace std;

int main(int argc, char *argv[])
{
    cout << HPCO_NAME << " Tester - A. Peternier (C) 2010 USI" << endl << endl;

   /* for (int d=0; d<500; d++)
    {
        RdtscTimerEntry::reset();
        cout << RdtscTimerEntry::getNumberOfEntries() << "\n";
    for (int c=0; c<5000; c++)
    RdtscTimerEntry::setEntry(10, 11, 12);
    cout << RdtscTimerEntry::getNumberOfEntries() << "\n";
    }*/

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
    cout << "   " << HpcOverseer::getNumberOfAvailableEvents() << " event(s) available" << endl;
    cout << "   " << HpcOverseer::getNumberOfInitializedEvents() << " event(s) initialized:" << endl;
    for (int c=0; c<HpcOverseer::getNumberOfInitializedEvents(); c++)
        cout << "      " << c << ". " << HpcOverseer::getInitializedEventString(c) << endl;

    // Attach events to main thread:
    pid_t pid = HpcOverseer::getThreadId();
    cout << "   Main thread PID: " << pid << endl;
    if (HpcOverseer::bindEventsToThread(pid) == false)
    {
        cout << "Unable to bind events to main thread" << endl;
        return 3;
    }

    // Read some events:
    cout << endl << "Reading events for " << TESTTIME << " seconds (ctrl+C to abort)..." << endl;
    int c=0;
    while (c < TESTTIME)
    {
        sleep(1);

        // Display some info:
        cout << endl << "   From thread: " << pid << endl;
        long v = HpcOverseer::getEventFromThread(pid, 0);
        cout << "      cache misses: " << v << endl;
        v = HpcOverseer::getEventFromThread(pid, 1);
        cout << "      cycles: " << v << endl;
        v = HpcOverseer::getEventFromThread(pid, 2);
        cout << "      instructions: " << v << endl;

        c++;
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
