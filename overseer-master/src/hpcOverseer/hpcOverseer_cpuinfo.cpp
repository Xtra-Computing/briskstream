/**
 * @file    hpcOverseer_cpuinfo.cpp
 * @brief	cpuinfo parser and data retriever
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "hpcOverseer.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <hwloc.h>
#include <hwloc/helper.h>
using namespace std;



///////////////////////
// COMMODITY METHODS //
///////////////////////

/**
 * Extract values from the form VAR : X
 * @param string input string in the form VAR : X
 * @return the value, or -1 on error
 */
/*static int extractValue(char *string)
{
    int value = -1;
    char buffer[HPCO_MAXLINE];

    // Extract the value string (if present):
    char *separator = strstr(string, ":");
    if (separator == NULL)
        return value;

    strcpy(buffer, separator+1);
    sscanf(buffer, "%d", &value);

    // Done:
    return value;
}*/



///////////////////////////////
// BODY OF CLASS HpcOverseer //
///////////////////////////////

/**
 * Opens and gathers information from system's cpuinfo file.
 * @return true or false on success/failure
 */
bool HpcOverseer::parseCpuInfo()
{
    // Gather easy data:
   /* numberOfCores = (int) sysconf(_SC_NPROCESSORS_ONLN);
    if (numberOfCores <= 0)
    {
        HPCO_LOGERROR("Invalid amount of Cores");
        return false;
    }

    // Read cpuinfo:
    FILE *dat = fopen("/proc/cpuinfo", "rt");

    // Read line by line and count maximum values:
    numberOfCpus = 0;
    numberOfCoresPerCpu = 0;
    char line[HPCO_MAXLINE];

    int physIdCache[HPCO_MAXLINE];  // Ok, I assume you've no more than HPCO_MAXLINE phys CPUs
    for (int c=0; c<HPCO_MAXLINE; c++)
        physIdCache[c] = -1;

    while (!feof(dat))
    {
        char *result;
        result = fgets(line, HPCO_MAXLINE, dat);
        int value = extractValue(line);

        // Update values:
        if (strstr(line, "physical id"))
        {
            // Check if already in the list:
            bool found = false;
            for (int c=0; c<HPCO_MAXLINE; c++)
                if (physIdCache[c] == value)
                {
                    found = true;
                    break;
                }
            if (!found)
            {
                numberOfCpus++;

                // Add it to the list:
                for (int c=0; c<HPCO_MAXLINE; c++)
                    if (physIdCache[c] == -1)
                    {
                        physIdCache[c] = value;
                        break;
                    }
            }
        }
        if (strstr(line, "cpu cores"))
            if (value > numberOfCoresPerCpu)
                numberOfCoresPerCpu = value;
    }

    // Read again, collect siblings info:
    fseek(dat, 0L, SEEK_SET);

    int cpuMapping[numberOfCpus];
    memset(cpuMapping, -1, sizeof(cpuMapping));
    int currentProcessor = -1;
    int currentPhysId = -1;

    // Init map:
    cpuCoreMapping = new int*[numberOfCpus];
	for(int i = 0; i < numberOfCpus; i++)
		cpuCoreMapping[i] = new int[numberOfCoresPerCpu];

    while (!feof(dat))
    {
        char *result;
        result = fgets(line, HPCO_MAXLINE, dat);
        int value = extractValue(line);

        // Update values:
        if (strstr(line, "processor"))
            currentProcessor = value;
        if (strstr(line, "physical id"))
            currentPhysId = value;
        if (strstr(line, "core id"))
        {
            // Find a place in the map:
            int mapPos = -1;
            for (int c=0; c<numberOfCpus; c++)
                if (cpuMapping[c] == currentPhysId)
                    mapPos = c;

            // Add new entry:
            if (mapPos == -1)
            {
                // Find a free slot:
                for (int c=0; c<numberOfCpus; c++)
                   if (cpuMapping[c] == -1)
                   {
                       cpuMapping[c] = currentPhysId;
                       mapPos = c;
                       break;
                   }
            }

            // Set the value:
            cpuCoreMapping[mapPos][value] = currentProcessor;
        }
    }

    // Done:
    fclose(dat);*/
    parseHwlocInfo();
    return true;
}


/**
 * Uses hwloc to gather information on the system specs.
 * @return true or false on success/failure
 */
bool HpcOverseer::parseHwlocInfo()
{
   hwloc_topology_t topology;
   hwloc_obj_t obj;
   int topodepth;

   // Init hwloc:
   hwloc_topology_init(&topology);
   hwloc_topology_load(topology);
   topodepth = hwloc_topology_get_depth(topology);

   // Get number of numas:
   int depth = hwloc_get_type_depth(topology, HWLOC_OBJ_NODE);
   if (depth != HWLOC_TYPE_DEPTH_UNKNOWN)
      numberOfNumas = hwloc_get_nbobjs_by_depth(topology, depth);

   // Get number of CPUs:
   depth = hwloc_get_type_depth(topology, HWLOC_OBJ_SOCKET);
   if (depth != HWLOC_TYPE_DEPTH_UNKNOWN)
   {
      numberOfCpus = hwloc_get_nbobjs_by_depth(topology, depth);
      if (numberOfNumas > 0)
         numberOfCpusPerNuma = numberOfCpus / numberOfNumas;

      /*UGLY FIX*/
      if (numberOfNumas > 0)
         numberOfCpus = numberOfNumas;
   }

   // Get number of Processing Units:
   depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);
   if (depth != HWLOC_TYPE_DEPTH_UNKNOWN)
      numberOfPus = hwloc_get_nbobjs_by_depth(topology, depth);

   // Get number of cores:
   depth = hwloc_get_type_depth(topology, HWLOC_OBJ_CORE);
   if (depth != HWLOC_TYPE_DEPTH_UNKNOWN)
   {
      numberOfCores = hwloc_get_nbobjs_by_depth(topology, depth);
      if (numberOfCpus > 0)
         numberOfCoresPerCpu = numberOfPus / numberOfCpus;
   }



   //////////////////////////
   // Reconstruct affinities:
   unsigned int depthPU = hwloc_get_type_or_below_depth(topology, HWLOC_OBJ_PU);
   unsigned int depthCpu = hwloc_get_type_or_below_depth(topology, HWLOC_OBJ_SOCKET);
   /*UGLY FIX:*/
   if (numberOfNumas > 0)
      depthCpu = hwloc_get_type_or_below_depth(topology, HWLOC_OBJ_NODE);

   // Init map:
   cpuCoreMapping = new int*[numberOfCpus];
	for(int i = 0; i < numberOfCpus; i++)
		cpuCoreMapping[i] = new int[numberOfCoresPerCpu];

   for (int c=0; c<numberOfPus; c++)
   {
      obj = hwloc_get_obj_by_depth(topology, depthPU, c);
      if (obj)
      {
         // printf("   PU#%d\n", c);
         // printf("      id: %d\n", obj->os_index);

         // Recursively get CPU:
         hwloc_obj_t parent = obj->parent;
         while (parent->depth != depthCpu)
         {
            parent = parent->parent;
            if (parent == NULL)
            {
               HPCO_LOGERROR("Corrupted topology tree");
               hwloc_topology_destroy(topology);
               return false;
            }
         }
         // printf("      parent cpu: %d\n", parent->os_index);
         cpuCoreMapping[parent->logical_index][c%numberOfCoresPerCpu] = obj->os_index; // first was os_index (! working on SC...)
      }
      else
      {
         HPCO_LOGERROR("Corrupted topology tree");
         hwloc_topology_destroy(topology);
         return false;
      }
   }

   // Free resources:
   hwloc_topology_destroy(topology);

   // Done:
   return true;
}


/**
 * Return the processor id of the CMP used in CPU cpu at position core.
 * @param cpu cpu id
 * @param core core id
 * @return return the processor id at position [cpu, core]
 */
const int HpcOverseer::getCpuCoreMapping(int cpu, int core)
{
    // Safety net:
    if (cpuCoreMapping == NULL)
    {
        HPCO_LOGERROR("HpcOverseer not initialized");
        return -1;
    }
    if (cpu<0 || cpu>=numberOfCpus || core<0 || core>=numberOfCoresPerCpu)
    {
        HPCO_LOGERROR("Invalid params");
        return -1;
    }

    // Done:
    return cpuCoreMapping[cpu][core];
}


