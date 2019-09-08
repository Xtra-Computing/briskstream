/**
 * @file    hpcOverseer_scheduler.cpp
 * @brief	HPC and CPU affinity access library
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "hpcOverseer.h"
#include <iostream>
#include <sched.h>
#include <perfmon/pfmlib_perf_event.h>
#include <sys/ptrace.h>
using namespace std;



////////////////////////
// SCHEDULING METHODS //
////////////////////////

/**
 * Set a thread affinity given its id and cpu mask.
 * @param pId thread id
 * @param mask binary mask specifying the cores to use
 * @return true on success, false otherwise
 */
bool HPCO_SetThreadAffinity(pid_t pId, unsigned long mask)
{
    if (sched_setaffinity(pId, sizeof(mask), (cpu_set_t *) &mask) == 0)
        return true;
    else
    {
        HPCO_LOGERROR("Unable to set affinity");
        return false;
    }
}

/**
 * Set a thread affinity given its pid and a cpu mask structure.
 * @param pId thread id
 * @param maskArray list of flags for each processor (0, 1)
 * @param size size of the previously passed array
 * @return true on success, false otherwise
 */
bool HPCO_SetThreadAffinityMask(pid_t pId, int *maskArray, int size)
{
   // Reset the affinity mask:
   cpu_set_t mask;
   CPU_ZERO(&mask);

   // Fill the mask from the array:
   for (int c=0; c<size; c++)
      if (maskArray[c])
         CPU_SET(c, &mask);

   // Apply the mask:
   if (sched_setaffinity(pId, sizeof(mask), &mask) == 0)
      return true;
   else
   {
      HPCO_LOGERROR("Unable to set affinity mask");
      return false;
   }
}

/**
 * Get a thread affinity mask.
 * @param pId thread id
 * @return binary mask specifying the cores being used, 0 in error
 */
long HPCO_GetThreadAffinity(pid_t pId)
{
    unsigned long mask;
    if (sched_getaffinity(pId, sizeof(mask), (cpu_set_t *) &mask) == 0)
        return mask;
    else
    {
        HPCO_LOGERROR("Unable to get affinity");
        return 0;
    }
}

/**
 * Get thread id running this method (once called).
 * @return thread id
 */
pid_t HpcOverseer::getThreadId()
{
    return (pid_t)syscall(__NR_gettid);
}

/**
 * Get the amount of threads for a pid
 * @return amount of threads
 */
int getAmountOfThreads(pid_t pid)
{
    return 1;
}
