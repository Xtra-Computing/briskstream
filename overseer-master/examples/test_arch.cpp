#include <stdlib.h>
#include <iostream>
#include "hpcOverseer.h"
using namespace std;

int main(int argc, char *argv[])
{
    cout << HPCO_NAME << " Test HW architecture - A. Peternier (C) 2010 USI" << endl << endl;

    // Init events:
    if (HpcOverseer::initialize() == false)
    {
        cout << "Unable to init HpcOverseer" << endl;
        return 1;
    }

    // Credit version:
    cout << "   Version: " << (HpcOverseer::getVersion() / 10.0) << endl;

    // Show HW config:
    cout << endl << "Hardware architecture detected: " << endl << endl;
    cout << "   " << HpcOverseer::getNumberOfNumas() << " numa node(s)" << endl;
    cout << "      " << HpcOverseer::getNumberOfCpusPerNuma() << " CPU(s) per node" << endl;
    cout << "   " << HpcOverseer::getNumberOfCpus() << " CPU(s)" << endl;
    cout << "      " << HpcOverseer::getNumberOfCoresPerCpu() << " core(s) per CPU" << endl;
    cout << "   " << HpcOverseer::getNumberOfCores() << " core(s) total" << endl;
    cout << "   " << HpcOverseer::getNumberOfPus() << " PU(s) total" << endl;
    cout << endl;

    // Show affinity relationships:
    cout << "CPU/core mapping: " << endl << endl;
    for (int c=0; c<HpcOverseer::getNumberOfCpus(); c++)
       for (int d=0; d<HpcOverseer::getNumberOfCoresPerCpu(); d++)
          cout << "   CPU " << c << " core " << d << " has processor id " << HpcOverseer::getCpuCoreMapping(c, d) << endl;

    // Shutdown:
    if (HpcOverseer::terminate() == false)
    {
        cout << "Unable to shutdown HpcOverseer" << endl;
        return 5;
    }
    cout << endl << "Application terminated" << endl;
    return 0;
}
