/**
 * @file    hpcOverseer_rdtsc.cpp
 * @brief	RDTSC timing manager
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "hpcOverseer.h"
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
using namespace std;



/////////////
// STATICS //
/////////////

    // Linked-list first element:
    RdtscTimerEntry *RdtscTimerEntry::firstEntry = NULL;
    RdtscTimerEntry *RdtscTimerEntry::lastEntry = NULL;



////////////////////
// NATIVE METHODS //
////////////////////

/**
 * Native access to RDTSC counter.
 * @return current RDTSC value
 */
inline unsigned long long int GetRDSTC()
{
#ifndef _ARCH_PPC
    unsigned long long int val;
    unsigned int __a,__d;
    asm volatile("rdtsc" : "=a" (__a), "=d" (__d));
    val = ((unsigned long)__a) | (((unsigned long)__d)<<32);
    return val;
#else
    return 0;
#endif
}



///////////////////////////////////
// BODY OF CLASS RdtscTimerEntry //
///////////////////////////////////

/**
 * Constructor.
 */
RdtscTimerEntry::RdtscTimerEntry() : paramA(0), paramB(0), paramC(0),
                                     rdtsc(0),
                                     prev(NULL), next(NULL)
{
    // Use shortcut?
    if (lastEntry)
    {
        lastEntry->next = this;
        prev = lastEntry;
    }
    else
    {
        // First item?
        if (firstEntry == NULL)
            firstEntry = this;
        else
        {
            // Get last item:
            RdtscTimerEntry *current = firstEntry;
            while (current->next != NULL)
                current = current->next;

            current->next = this;
            prev = current;
        }
    }
    lastEntry = this;

    HPCO_LOG("RdtscTimerEntry added");
}


/**
 * Destructor.
 */
RdtscTimerEntry::~RdtscTimerEntry()
{
    // First item?
    if (prev == NULL)
    {
        firstEntry = next;
        if (next)
           next->prev = NULL;
    }
    else
    {
        prev->next = next;
        if (next)
           next->prev = prev;
    }

    // Reset cache:
    lastEntry = NULL;

    HPCO_LOG("RdtscTimerEntry removed");
}


/**
 * Return the amount of elements in the chained-list.
 * @return amount
 */
int RdtscTimerEntry::getNumberOfEntries()
{
    int numberOfEntries = 0;

    RdtscTimerEntry *current = firstEntry;
    while (current)
    {
        numberOfEntries++;
        current = current->next;
    }

    // Done:
    return numberOfEntries;
}


/**
 * Clean up.
 */
void RdtscTimerEntry::reset()
{
    RdtscTimerEntry *current = firstEntry;
    while (current)
    {
        RdtscTimerEntry *_temp = current->next;
        delete current;
        current = _temp;
    }
    firstEntry = NULL;
    lastEntry = NULL;
}


/**
 * Add an entry.
 * @param paramA free param A
 * @param paramB free param B
 * @param paramC free param C
 */
void RdtscTimerEntry::setEntry(int paramA, int paramB, int paramC)
{
    RdtscTimerEntry *rte = new RdtscTimerEntry();
    rte->rdtsc = GetRDSTC();
    rte->paramA = paramA;
    rte->paramB = paramB;
    rte->paramC = paramC;
}


/**
 * Log entries to file.
 * @param filename output file
 * @return true on success, false otherwise
 */
bool RdtscTimerEntry::logToFile(const char *filename)
{
    FILE *dat = fopen(filename, "wt");
    if (dat == NULL)
    {
        HPCO_LOGERROR("Unable to open file " << filename);
        return false;
    }

    // Dump all data:
    RdtscTimerEntry *current = firstEntry;
    while (current)
    {
        fprintf(dat, "%llu %d %d %d\n", current->rdtsc, current->paramA, current->paramB, current->paramC);
        current = current->next;
    }

    // Done:
    fclose(dat);
    return true;
}



