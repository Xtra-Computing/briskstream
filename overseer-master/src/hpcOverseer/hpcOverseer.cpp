/**
 * @file    hpcOverseer.cpp
 * @brief	HPC and CPU affinity access library
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "hpcOverseer.h"
#include <stdio.h>
#include <iostream>
#include <sys/types.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <err.h>
#include <regex.h>
#include <string.h>
#include <sys/prctl.h>
#include <perfmon/pfmlib.h>
#include <perfmon/pfmlib_perf_event.h>
using namespace std;



/////////////
// #DEFINE //
/////////////

    // Lookup shortcut
    #define GETTHREAD(x) reserved->thread[reserved->lookupTable[x]]



/////////////
// STATICS //
/////////////

    // Flags:
    bool HpcOverseer::initialized = false;

    // Config:
    int HpcOverseer::numberOfNumas       = 0;
    int HpcOverseer::numberOfCpusPerNuma = 0;
    int HpcOverseer::numberOfCores       = 0;
    int HpcOverseer::numberOfCpus        = 0;
    int HpcOverseer::numberOfCoresPerCpu = 0;
    int HpcOverseer::numberOfPus         = 0;
    int HpcOverseer::numberOfThreads     = 0;
    int HpcOverseer::numberOfEvents      = 0;
    int HpcOverseer::numberOfAvailableEvents = 0;
    int **HpcOverseer::cpuCoreMapping    = NULL;
    char *HpcOverseer::availableEvent    = NULL;

    // Reserved structure:
    void *HpcOverseer::_reserved = NULL;



////////////////////////
// RESERVED STRUCTURE //
////////////////////////

typedef struct
{
	struct perf_event_attr hw;
	uint64_t value, prev_value;
	char name[HPCO_MAXLINE];
	uint64_t id;
	void *buf;
	size_t pgmsk;
	int fd;
	int loadedEvents;
} perf_event_desc_t;

struct HPCO_Reserved
{
    perf_event_desc_t *fd;
    perf_event_desc_t **cpu;
    perf_event_desc_t **thread;
};
#define HPCO_ACCESS_RESERVED HPCO_Reserved *reserved = (HPCO_Reserved *) _reserved



///////////////////////////////
// BODY OF CLASS HpcOverseer //
///////////////////////////////

/**
 * Initializer (call it once per application).
 * @return true or false on success/failure
 */
bool HpcOverseer::initialize()
{
    // Safety net:
    if (initialized)
    {
        HPCO_LOGERROR("Already initialized");
        return false;
    }

    // Get config data:
    if (parseCpuInfo() == false)
    {
        HPCO_LOGERROR("Unable to parse CPU info");
        return false;
    }
    HPCO_LOG(numberOfCores << " core(s) found");
    HPCO_LOG(numberOfCpus << " CPU(s) found");
    HPCO_LOG(numberOfCoresPerCpu << " core(s) per CPU found");

    // Read pidmax:
    int error;
    FILE *dat = fopen("/proc/sys/kernel/pid_max", "rt");
    error = fscanf(dat, "%d", &numberOfThreads);
    fclose(dat);
    if (error <= 0)
    {
        HPCO_LOGERROR("Unable to read PID_MAX value");
        return false;
    }
    HPCO_LOG(numberOfThreads << " threads max");

    // Init perfmon:
    error = pfm_initialize();
    if (error != PFM_SUCCESS)
    {
        HPCO_LOGERROR("Unable to initialize pfm (error: " << pfm_strerror(error) << ")");
        return false;
    }
    int version = pfm_get_version();
    version = version; // <-- avoid a warning
    HPCO_LOG("PerfMon version " << PFM_MAJ_VERSION(version) << "." << PFM_MIN_VERSION(version) << " initialized");

    // Init static structures:
    _reserved = new HPCO_Reserved();
    HPCO_ACCESS_RESERVED;
    numberOfEvents = 0;
    reserved->fd = NULL;
    reserved->cpu = new perf_event_desc_t*[numberOfCores];
    for (int c=0; c<numberOfCores; c++)
        reserved->cpu[c] = NULL;
    reserved->thread = new perf_event_desc_t*[numberOfThreads];

    for (int c=0; c<numberOfThreads; c++)
        reserved->thread[c] = NULL;

    // Get the available event list:
    regex_t preg;
    if (regcomp(&preg, ".*", REG_ICASE|REG_NOSUB))
    {
        HPCO_LOGERROR("Unable to acquire event list");
        return false;
    }

    // Parse list:
    pfm_pmu_info_t pinfo;
	pfm_event_info_t info;
	int i, j, ret;

	memset(&pinfo, 0, sizeof(pinfo));
	memset(&info, 0, sizeof(info));
	numberOfAvailableEvents = 0;

   // Iterate:
   string output = "";
   pfm_for_all_pmus(j)
   {
		ret = pfm_get_pmu_info((pfm_pmu_t) j, &pinfo);
		if (ret != PFM_SUCCESS)
         continue;

      if (!pinfo.is_present)
			continue;
      HPCO_LOG("Detected: " << pinfo.name << ", " << pinfo.desc);

      for (i=pinfo.first_event; i != -1; i = pfm_get_event_next((i)))
      {
         ret = pfm_get_event_info(i, PFM_OS_NONE, &info);
         if (ret != PFM_SUCCESS)
         {
            HPCO_LOGERROR("Cannot get event info: " << pfm_strerror(ret));
         }

         if (info.pmu != pinfo.pmu)
            continue;
         numberOfAvailableEvents++;
         if (output == "")
               output += info.name;
           else
           {
               output += "\n";
               output += info.name;
           }
      }
   }
	availableEvent = (char *) malloc(strlen(output.c_str())+1);
	strcpy(availableEvent, output.c_str());

    // Done:
    initialized = true;
    HPCO_LOG("Initialized");
    return true;
}


/**
 * Deinitializer.
 * @return true or false on success/failure
 */
bool HpcOverseer::terminate()
{
    // Safety net:
    if (!initialized)
    {
        HPCO_LOGERROR("Not initialized");
        return false;
    }

    // Release static structures:
    if (_reserved)
    {
        HPCO_ACCESS_RESERVED;
        if (reserved->fd)
            free(reserved->fd);
        if (reserved->cpu)
            for (int c=0; c<numberOfCores; c++)
                if (reserved->cpu[c])
                    delete reserved->cpu[c];
        delete [] reserved->cpu;
      if (reserved->thread)
            for (int c=0; c<numberOfThreads; c++)
                if (reserved->thread[c])
                {
                   int _loadedEvents = reserved->thread[c][0].loadedEvents;
                   for (int i=0; i<_loadedEvents; i++)
                     close(reserved->thread[c][i].fd);
                    delete [] reserved->thread[c];
                }

        delete [] reserved->thread;
        delete (HPCO_Reserved *) _reserved;
        _reserved = NULL;
    }

    int error = prctl(PR_TASK_PERF_EVENTS_DISABLE);
	if (error)
		HPCO_LOGERROR("prctl(disable) failed");

    if (cpuCoreMapping)
    {
        for(int i = 0; i < numberOfCpus; i++)
            delete [] cpuCoreMapping[i];
        delete [] cpuCoreMapping;
        cpuCoreMapping = NULL;
    }

    if (availableEvent)
    {
        free(availableEvent);
        availableEvent = NULL;
    }

    // Done:
    initialized = false;
    HPCO_LOG("Released");
    return true;
}


/**
 * Get compiled version number.
 * @return version number (divide by 10)
 */
const int HpcOverseer::getVersion()
{
    return (int) (HPCO_VERSION * 10.0);
}


/**
 * Get amount of detected numas.
 * @return amount of numas
 */
const int HpcOverseer::getNumberOfNumas()
{
    return numberOfNumas;
}


/**
 * Get amount of detected cpus per numa.
 * @return amount of cpus per numa node
 */
const int HpcOverseer::getNumberOfCpusPerNuma()
{
    return numberOfCpusPerNuma;
}


/**
 * Get amount of detected cores.
 * @return amount of cores
 */
const int HpcOverseer::getNumberOfCores()
{
    return numberOfCores;
}


/**
 * Get amount of detected CPUs.
 * @return amount of CPUs
 */
const int HpcOverseer::getNumberOfCpus()
{
    return numberOfCpus;
}


/**
 * Get amount of detected cores per CPU.
 * @return amount of cores per CPU
 */
const int HpcOverseer::getNumberOfCoresPerCpu()
{
    return numberOfCoresPerCpu;
}



/**
 * Get amount of detected PUs.
 * @return amount of PUs
 */
const int HpcOverseer::getNumberOfPus()
{
    return numberOfPus;
}


/**
 * Get amount of initialized events:
 * @return amount of events
 */
const int HpcOverseer::getNumberOfInitializedEvents()
{
    return numberOfEvents;
}


/**
 * Get amount of available counters.
 * @return amount of counters
 */
const int HpcOverseer::getNumberOfAvailableEvents()
{
     // Safety net:
    if (!initialized)
    {
        HPCO_LOGERROR("Not initialized");
        return -1;
    }

    // Done:
    return numberOfAvailableEvents;
}


/**
 * Get the max amount of threads (max_pid)
 * @return max_pid
 */
const int HpcOverseer::getNumberOfThreads()
{
    return numberOfThreads;
}


/**
 * Get the string about a currently initialized event.
 * @param event event number
 * @return string about the event initialized
 */
const char *HpcOverseer::getInitializedEventString(int event)
{
    // Safety net:
    if (event < 0 || event >= numberOfEvents)
    {
        HPCO_LOGERROR("Invalid params");
        return NULL;
    }

    HPCO_ACCESS_RESERVED;

    // Done:
    return reserved->fd[event].name;
}


/**
 * Get an OpenGL-extension-like string with the supported events.
 * @return string with sequential events listed
 */
 const char *HpcOverseer::getAvailableEventsString()
 {
     // Safety net:
     if (!initialized)
    {
        HPCO_LOGERROR("Not initialized");
        return NULL;
    }

    // Done:
    return availableEvent;
}


/**
 * Setup a list of events.
 * @param ev string containing the events to listen
 * @return success or failure
 */
bool HpcOverseer::initEvents(const char *event)
{
    // Safety net:
    if (event == NULL)
    {
        HPCO_LOGERROR("Invalid params");
        return false;
    }

    // Allocate working specimen:
    char *_event;
    _event = strdup(event);
    if (_event == NULL)
    {
        HPCO_LOGERROR("Internal error");
        return false;
    }

    // Remove spaces:
    char *p = _event, *q = _event;
    while (*p != 0)
    {
        if ((*p) == ' ')
            p++;
        else
            *q++ = *p++;
    }
    *q = 0;

    // Count passed arguments (space separated):
    int argc = 1;
    q = _event;
    while ((p = strchr(q, ',')))
    {
		argc++;
		q = p + 1;
	}

	HPCO_LOG(argc << " args found");

	// Create args list:
	char **argv = (char **) malloc(argc * sizeof(char *));

    // Extract params:
    q = _event;
	for (int c=0; c<argc-1; c++)
	{
		p = strchr(q, ',');
		*p = '\0';
		argv[c] = q;
		q = p + 1;
	}
	argv[argc-1] = q;

	// List is ready:
	for (int c=0; c<argc; c++)
        HPCO_LOG("Param " << c << ": " << argv[c]);

    // Register events:
    HPCO_ACCESS_RESERVED;
    if (reserved->fd)
    {
        free(reserved->fd);
        reserved->fd = NULL;
        numberOfEvents = 0;
    }

	int num = 0, max_fd = 0, new_max;
	int error;

    for (int c=0; c<argc; c++)
    {
    	if (num == max_fd)
    	{

			if (!max_fd)
				new_max = 2;
			else
				new_max = max_fd << 1;

            // Too many entries?
			if (new_max < max_fd)
			{
			    HPCO_LOGERROR("Too many entries");
				goto error;
			}

			reserved->fd = (perf_event_desc_t *) realloc(reserved->fd, new_max * sizeof(*reserved->fd));

			// Cannot allocate memory?
			if (!reserved->fd)
			{
			    HPCO_LOGERROR("Cannot allocate memory");
				goto error;
			}

			// Reset newly allocate mem:
			memset(reserved->fd+max_fd, 0, (new_max - max_fd) * sizeof(*reserved->fd));
			max_fd = new_max;
		}

      memset(&reserved->fd[num].hw, 0, sizeof(reserved->fd[num].hw));
		error = pfm_get_perf_event_encoding(argv[c], PFM_PLM3, &reserved->fd[num].hw, NULL, NULL);

		// Unable to register this event:
		if (error != PFM_SUCCESS)
		{
		    HPCO_LOGERROR("event: " << argv[c] << ", error: " << pfm_strerror(error));
			goto error;
		}

		// ABI comptatibility:
		reserved->fd[num].hw.size = sizeof(struct perf_event_attr);
		strcpy(reserved->fd[num].name, argv[c]);
		num++;
	}

	// Free some resources:
	free(argv);

	// Done:
	numberOfEvents = argc;
	return true;

error:
    free(argv);
    if (reserved->fd)
    {
        free(reserved->fd);
        reserved->fd = NULL;
    }
    numberOfEvents = 0;
    return false;
}


/**
 * Attach the previously loaded list of events (through initEvents) to a specific core.
 * @param coreId core id
 * @return true on success, false otherwise
 */
bool HpcOverseer::bindEventsToCore(int coreId)
{
   // Safety net:
   if (coreId < 0 || coreId >= numberOfCores)
   {
      HPCO_LOGERROR("Invalid params");
      return false;
   }

   HPCO_ACCESS_RESERVED;

   // Already in use?
   if (reserved->cpu[coreId])
   {
      HPCO_LOG("Reusing cpu " << coreId);
      int _loadedEvents = reserved->cpu[coreId][0].loadedEvents;
      for (int i=0; i < _loadedEvents; i++)
         close(reserved->cpu[coreId][i].fd);
      delete [] reserved->cpu[coreId];
      reserved->cpu[coreId] = NULL;
   }

   // Create a copy:
   perf_event_desc_t *newS = new perf_event_desc_t[numberOfEvents];
   for (int c=0; c<numberOfEvents; c++)
      memcpy(&newS[c], &reserved->fd[c], sizeof(perf_event_desc_t));
   reserved->cpu[coreId] = newS;
   int amount = 0;

   reserved->cpu[coreId][0].loadedEvents = numberOfEvents;
	for (int i=0; i < numberOfEvents; i++)
	{
      reserved->cpu[coreId][i].hw.disabled = 0;
		//reserved->cpu[cpuId][i].hw.exclusive = 1;

		// Request timing information necessary for scaling counts:
		reserved->cpu[coreId][i].hw.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED|PERF_FORMAT_TOTAL_TIME_RUNNING;
		reserved->cpu[coreId][i].fd = perf_event_open(&reserved->cpu[coreId][i].hw, -1, coreId, -1, 0);
		if (reserved->cpu[coreId][i].fd == -1)
         HPCO_LOGERROR("Cannot attach event " << reserved->cpu[coreId][i].name << " to core " << coreId);
      else
         amount++;
	}

   // Done:
   HPCO_LOG(amount << " events attached to core " << coreId);
   return true;
}


/**
 * Attach the previously loaded list of events (through initEvents) to a specific thread.
 * @param pId thread id
 * @return true on success, false otherwise
 */
bool HpcOverseer::bindEventsToThread(pid_t pId)
{
   HPCO_ACCESS_RESERVED;

   // Already used?
   if (reserved->thread[pId])
   {
      HPCO_LOG("Reusing thread id " << pId);
      int _loadedEvents = reserved->thread[pId][0].loadedEvents;
      for (int i=0; i < _loadedEvents; i++)
         close(reserved->thread[pId][i].fd);
      delete [] reserved->thread[pId];
      reserved->thread[pId] = NULL;
   }

   // Create a copy:
   perf_event_desc_t *newS = new perf_event_desc_t[numberOfEvents];
   for (int c=0; c<numberOfEvents; c++)
      memcpy(&newS[c], &reserved->fd[c], sizeof(perf_event_desc_t));
   reserved->thread[pId] = newS;
   int amount = 0;

   reserved->thread[pId][0].loadedEvents = numberOfEvents;
	for (int i=0; i < numberOfEvents; i++)
	{
      reserved->thread[pId][i].hw.disabled = 0;
		reserved->thread[pId][i].hw.sample_type =  PERF_SAMPLE_IP|PERF_SAMPLE_TID;

		// Request timing information necessary for scaling counts:
		reserved->thread[pId][i].hw.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED|PERF_FORMAT_TOTAL_TIME_RUNNING;
		reserved->thread[pId][i].fd = perf_event_open(&reserved->thread[pId][i].hw, pId, -1, -1, 0);
		if (reserved->thread[pId][i].fd == -1)
         HPCO_LOGERROR("Cannot attach event " << reserved->thread[pId][i].name << " to thread " << pId);
      else
         amount++;
	}

   // Done:
   HPCO_LOG(amount << " events attached to thread " << pId);
   return true;
}


/**
 * Start retriving events.
 * @return true on success, false otherwise
 */
bool HpcOverseer::start()
{
    // Activate counters:
	int error = prctl(PR_TASK_PERF_EVENTS_ENABLE);
	if (error)
	{
		HPCO_LOGERROR("prctl(enable) failed");
		return false;
	}

	// Done:
	return true;
}


/**
 * Stop collecting events.
 * @return true on success, false otherwise
 */
bool HpcOverseer::stop()
{
     // Activate counters:
	int error = prctl(PR_TASK_PERF_EVENTS_DISABLE);
	if (error)
	{
		HPCO_LOGERROR("prctl(disable) failed");
		return false;
	}

	// Done:
	return true;
}


/**
 * Get value returned by a specific event on a core.
 * @param coreId core id
 * @param event event number
 * @return value
 */
long HpcOverseer::getEventFromCore(int coreId, int event)
{
    HPCO_ACCESS_RESERVED;
    int error;
    long retValue = 0;
    perf_event_desc_t *fds;

    // Safety net:
    if (reserved->cpu[coreId] == NULL || event < 0 || event >= reserved->cpu[coreId][0].loadedEvents || coreId < 0 || coreId >= getNumberOfCores())
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Sum values returned from CPUs:
    uint64_t values[3];
    fds = reserved->cpu[coreId];
    error = read(fds[event].fd, values, sizeof(values));
    if (error != sizeof(values))
    {
        if (error == -1)
            HPCO_LOGERROR("cannot read event");
        else
            HPCO_LOGERROR("could not read event");
        return 0;
    }

    if (values[2])
       retValue = (uint64_t)((double)values[0] * values[1]/values[2]);

    // Done:
    return retValue;
}


/**
 * Get value returned by a specific event on a thread.
 * @param pId thread id
 * @param event event number
 * @return value
 */
long HpcOverseer::getEventFromThread(pid_t pId, int event)
{
    HPCO_ACCESS_RESERVED;
    int error;
    long retValue = 0;
    perf_event_desc_t *fds;

    // Safety net:
    if (reserved->thread[pId] == NULL || event < 0 || event >= reserved->thread[pId][0].loadedEvents)
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Sum values returned from CPUs:
    uint64_t values[3];
    fds = reserved->thread[pId];
    error = read(fds[event].fd, values, sizeof(values));
    if (error != sizeof(values))
    {
        if (error == -1)
            HPCO_LOGERROR("cannot read event");
        else
            HPCO_LOGERROR("could not read event");
        return 0;
    }

    if (values[2])
      retValue = (uint64_t)((double)values[0] * values[1]/values[2]);

    // Done:
    return retValue;
}


/**
 * Get number of tracked counters on a specific core.
 * @param coreId id
 * @return value
 */
int HpcOverseer::getNumberOfEventsFromCore(int coreId)
{
    HPCO_ACCESS_RESERVED;

    // Safety net:
    if (reserved->cpu[coreId] == NULL || coreId < 0 || coreId >= getNumberOfCores())
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Done:
    return reserved->cpu[coreId][0].loadedEvents;
}


/**
 * Get number of tracked counters on a specific thread.
 * @param pId thread id
 * @return value
 */
int HpcOverseer::getNumberOfEventsFromThread(pid_t pId)
{
    HPCO_ACCESS_RESERVED;

    // Safety net:
    if (reserved->thread[pId] == NULL)
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Done:
    return reserved->thread[pId][0].loadedEvents;
}


/**
 * Get name of tracked counters on a specific core.
 * @param coreId core id
 * @param event event number
 * @return value
 */
char *HpcOverseer::getEventNameFromCore(int coreId, int event)
{
    HPCO_ACCESS_RESERVED;

    // Safety net:
    if (reserved->cpu[coreId] == NULL || event < 0 || event >= reserved->cpu[coreId][0].loadedEvents)
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Done:
    return reserved->cpu[coreId][event].name;
}


/**
 * Get name of tracked counters on a specific thread.
 * @param pId thread id
 * @param event event number
 * @return value
 */
char *HpcOverseer::getEventNameFromThread(pid_t pId, int event)
{
    HPCO_ACCESS_RESERVED;

    // Safety net:
    if (reserved->thread[pId] == NULL || event < 0 || event >= reserved->thread[pId][0].loadedEvents)
    {
        HPCO_LOGERROR("Invalid params");
        return 0;
    }

    // Done:
    return reserved->thread[pId][event].name;
}
