/**
 * @file    threadList.cpp
 * @brief	keep a list of all the threads currently running within the JVM
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "threadList.h"
#include "smartLock.h"
#include "overAgent.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>



////////////
// GLOBAL //
////////////

   // From "overAgent.cpp":
   extern GlobalAgentData gdata;



////////////
// STATIC //
////////////

   // Log:
   FILE *ThreadList::log = NULL;

   // Linked-list:
   ThreadList *ThreadList::firstElement = NULL;
   pthread_mutex_t ThreadList::mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

   // Stats:
   unsigned long ThreadList::totalJiffies = 0;
   unsigned long ThreadList::totalJiffiesRelative = 0;



/////////////
// METHODS //
/////////////

/**
 * Constructor.
 */
ThreadList::ThreadList() : pid(0), tid(0), javaThread(0),
                           status(THREAD_STATUS_UNDEFINED),
                           daemon(false),
                           jiffies(0), cpuUsage(0.0f), cpuUsageRelative(0.0f),
                           prev(NULL), next(NULL)
{
   MUTEX_SYNC(mutex);
   strcpy(name, "[unknown]");
   strcpy(filename, "[unknown]");

   // Add to the linked list:
   if (firstElement == NULL)
      firstElement = this;
   else
   {
      ThreadList *current = firstElement;

      while (current->next != NULL)
         current = current->next;

      current->next = this;
      prev = current;
   }
}

/**
 * Constructor.
 * @param pid process id
 * @param tid thread id
 */
ThreadList::ThreadList(pid_t _pid, pid_t _tid) : pid(_pid), tid(_tid), javaThread(0),
                                     status(THREAD_STATUS_UNDEFINED),
                                     jiffies(0), cpuUsage(0.0f), cpuUsageRelative(0.0f),
                                     prev(NULL), next(NULL)
{
   MUTEX_SYNC(mutex);
   strcpy(name, "[unknown]");
   sprintf(filename, "/proc/%u/task/%u/stat", pid, tid);

   // Add to the linked list:
   if (firstElement == NULL)
      firstElement = this;
   else
   {
      ThreadList *current = firstElement;

      while (current->next != NULL)
         current = current->next;

      current->next = this;
      prev = current;
   }
}

/**
 * Destructor.
 */
ThreadList::~ThreadList()
{
   MUTEX_SYNC(mutex);

   // Remove from the linked list:
   if (prev == NULL)
   {
		firstElement = next;
		if (next)
			next->prev = NULL;
   }
   else
   {
		prev->next = next;
      if (next)
			next->prev = prev;
   }

   // Log its glorious achievements:
   if (LOG_ENABLE && log!=NULL)
      fprintf(log, "Thread: %s, daemon: %d\n", name, daemon);

   // Well, embarassing...:
   if (status != THREAD_STATUS_TERMINATIONNOTIFIED)
      printf("ERROR: thread %d:%d [%s] terminated without notification\n", pid, tid, name);
}

/**
 * Cleanup the list.
 */
void ThreadList::clear()
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
		return;

	// Free all objects:
   ThreadList *current = firstElement;
   while (current)
   {
		ThreadList *temp = current->next;
		delete current;

      // Next one:
      current = temp;
   }

   // Done:
	firstElement = NULL;
}

/**
 * Get number of threads in the list.
 * @return number of threads
 */
unsigned int ThreadList::getNumberOf()
{
   MUTEX_SYNC(mutex);

	// Necessary?
   if (firstElement == NULL)
      return 0;

	// Count elements:
   unsigned int counter = 0;
   ThreadList *current = firstElement;
   while (current)
   {
      counter++;
      // printf("%d) %s %d\n", counter, current->getName(), current->getPid());
      current = current->next;
   }

   // Done:
   return counter;
}

/**
 * Get pointer to an element given its tid.
 * @return element pointer or NULL if not found
 */
ThreadList *ThreadList::findByTid(pid_t tid)
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return NULL;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      if (current->tid == tid)
         return current;
      current = current->next;
   }

   // Not found:
   return NULL;
}

/**
 * Get pointer to an element given its name.
 * @return element pointer or NULL if not found
 */
ThreadList *ThreadList::findByName(char *name)
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return NULL;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      if (strcpy(current->getName(), name) == 0)
         return current;
      current = current->next;
   }

   // Not found:
   return NULL;
}

/**
 * Update the names in the list of threads.
 * Typically invoked after VM init message is received to update [unknown] fields.
 */
void ThreadList::update()
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      get_thread_name(gdata.jvmti, current->javaThread, current->name, MAX_THREADNAMELENGTH - 1);
      current->daemon = is_thread_daemon(gdata.jvmti, current->javaThread);
      current = current->next;
   }
}

/**
 * Force notification of unnotified threads.
 * Typically invoked after VM init message is received to update [unknown] fields.
 */
void ThreadList::notify(JNIEnv* jni_env)
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      switch (current->status)
      {
         // Undefined -> creation notified:
         case THREAD_STATUS_UNDEFINED:
            {
               if (gdata.vm_is_started)
               {
                  jstring callbackArg = jni_env->NewStringUTF(current->getName());
                  jni_env->CallStaticVoidMethod(gdata.overAgentClass, gdata.callbackJava, gdata.THREAD_CREATED, current->getTid(), callbackArg);
                  current->setStatus(THREAD_STATUS_CREATIONNOTIFIED);
               }
            }
            break;

         // Creation notified -> Termination notified:
         case THREAD_STATUS_CREATIONNOTIFIED:
            {
               if (!gdata.vm_is_started)
               {
                  jstring callbackArg = jni_env->NewStringUTF(current->getName());
                  jni_env->CallStaticVoidMethod(gdata.overAgentClass, gdata.callbackJava, gdata.THREAD_TERMINATED, current->getTid(), callbackArg);
                  current->setStatus(THREAD_STATUS_TERMINATIONNOTIFIED);
               }
            }
            break;

         // Well, embarassing...:
         default:
            printf("ERROR: undefined status at notify\n");
      }
      current = current->next;
   }
}

/**
 * Get the cpu usage of an element given its tid.
 * @return cpu usage or -1.0f if not found
 */
float ThreadList::getCpuUsageByTid(pid_t tid)
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return NULL;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      if (current->tid == tid)
         return current->cpuUsage;
      current = current->next;
   }

   // Not found:
   return -1.0f;
}

/**
 * Get the relative cpu usage of an element given its tid.
 * @return cpu relative usage or -1.0f if not found
 */
float ThreadList::getCpuUsageRelativeByTid(pid_t tid)
{
   MUTEX_SYNC(mutex);

   // Necessary?
   if (firstElement == NULL)
      return NULL;

   // Iterate through elements:
   ThreadList *current = firstElement;
   while (current)
   {
      if (current->tid == tid)
         return current->cpuUsageRelative;
      current = current->next;
   }

   // Not found:
   return -1.0f;
}

/**
 * Open log file.
 */
void ThreadList::initLog()
{
   if (LOG_ENABLE)  
      if (log == NULL)
      {
      	 log = fopen(LOG_FILE, "wt");
         if (log == NULL)
         {
	    printf("ERROR: unable to open log file '%s'\n", LOG_FILE);
         }
      }
}

/**
 * Close log file.
 */
void ThreadList::closeLog()
{
   if (LOG_ENABLE)  
      if (log)
      {
      	 fclose(log);
         log = NULL;
      }
}

/**
 * Update thread usage stats.
 * @return TF
 */
bool ThreadList::updateStats()
{
   MUTEX_SYNC(mutex);

   // Baseline reference on /proc/stat:
   int refFile = open("/proc/stat", O_RDONLY | O_NONBLOCK, 0);
   char refBuffer[MAX_THREADSTATBUFFERSIZE];
   int refNumRead = read(refFile, refBuffer, MAX_THREADSTATBUFFERSIZE-1);
   refBuffer[refNumRead] = '\0';
   close(refFile);

   // Get buffer for all threads:
   ThreadList *current = firstElement;
   while (current)
   {
		current->file = open(current->filename, O_RDONLY | O_NONBLOCK, 0);
		if (current->file == -1)
		{
		   printf("ERROR: unable to open %s for stat updating\n", current->filename);
		   current = current->next;
		   continue;
		}

      int numRead = read(current->file, current->buffer, MAX_THREADSTATBUFFERSIZE-1);
	   if (numRead == -1)
	   {
	      printf("ERROR: unable to read from file %s while updating\n", current->filename);
	      close(current->file);
	      current->file = -1;
	      current = current->next;
	      continue;
	   }

	   current->buffer[numRead] = '\0';
	   close(current->file);

      // Next one:
      current = current->next;
   }

   // Snapshots from /proc/stat* token, now process them:
   unsigned long cat[THREAD_JIFFY_LAST];
	sscanf(refBuffer, "%*s %lu %lu %lu %lu %lu %lu %lu %lu %lu", &cat[THREAD_JIFFY_STDPROCUSERMODE],
                                                                &cat[THREAD_JIFFY_NICEPROCUSERMODE],
                                                                &cat[THREAD_JIFFY_SYSTPROCKERNMODE],
                                                                &cat[THREAD_JIFFY_IDLE],
                                                                &cat[THREAD_JIFFY_IOWAIT],
                                                                &cat[THREAD_JIFFY_IRQ],
                                                                &cat[THREAD_JIFFY_SOFTIRQ],
                                                                &cat[THREAD_JIFFY_STEALTIME],
                                                                &cat[THREAD_JIFFY_VIRTUALQUEST]);

   // Update absolute reference values:
   unsigned long previousTotal = totalJiffies;
   totalJiffies = 0;
   for (int c=0; c<THREAD_JIFFY_LAST; c++)
      totalJiffies += cat[c];
   unsigned long delta = totalJiffies - previousTotal;

   // Update relative reference values:
   unsigned long previousTotalRelative = totalJiffiesRelative;
   totalJiffiesRelative = 0;
   for (int c=0; c<2; c++)
      totalJiffiesRelative += cat[c];
   unsigned long deltaRelative = totalJiffiesRelative - previousTotalRelative;

   // Finally, update all per-thread values:
   current = firstElement;
   while (current)
   {
      // In case of problem, skip this one:
      if (current->file == -1)
      {
            current->cpuUsage = -1.0f;
            current->cpuUsageRelative = -1.0f;
            current = current->next;
            continue;
      }

      // Store previous value:
      unsigned long previousJiffies = current->jiffies;

      // Process buffer:
      char* ptrUsr = strrchr(current->buffer, ')') + 1;
      for (int i=3 ; i!=14; ++i)
         ptrUsr = strchr(ptrUsr+1, ' ');
      ptrUsr++;
      long jiffies_user = atol(ptrUsr);
      long jiffies_sys = atol(strchr(ptrUsr,' ') + 1);
      current->jiffies = jiffies_user + jiffies_sys;
      current->jiffies = jiffies_user;

      // Compute stats:
      unsigned long threadDelta = current->jiffies - previousJiffies;
      if (delta == 0)
      {
         current->cpuUsage = 0.0f;
         current->cpuUsageRelative = 0.0f;
      }
      else
      {
         current->cpuUsage = (float) (((double) threadDelta / (double) delta) * 100.0);
         current->cpuUsageRelative = (float) (((double) threadDelta / (double) deltaRelative) * 100.0);
      }

      // Next one:
      current = current->next;
   }

   // Done:
   return true;
}
