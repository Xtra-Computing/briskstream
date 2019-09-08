/**
 * @file    threadList.h
 * @brief	keep a list of all the threads currently running within the JVM
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */
#ifndef THREADLIST_H_INCLUDED
#define THREADLIST_H_INCLUDED



//////////////
// #INCLUDE //
//////////////
#include <jvmti.h>
#include <pthread.h>
#include <string.h>



/////////////
// #DEFINE //
/////////////

   // Limits:
   #define MAX_THREADNAMELENGTH 256
   #define MAX_THREADFILENAMELENGTH 64
   #define MAX_THREADSTATBUFFERSIZE 512

   // Logging:
   #define LOG_ENABLE true
   #define LOG_FILE "overAgent.log"

   // Thread status:
   enum ThreadStatus
   {
      THREAD_STATUS_UNDEFINED = 0,
      THREAD_STATUS_CREATIONNOTIFIED,
      THREAD_STATUS_TERMINATIONNOTIFIED
   };

   // /proc/stat categories of jiffies:
   enum THREAD_JIFFY_CATEGORY
   {
      THREAD_JIFFY_STDPROCUSERMODE = 0,
      THREAD_JIFFY_NICEPROCUSERMODE,
      THREAD_JIFFY_SYSTPROCKERNMODE,
      THREAD_JIFFY_IDLE,
      THREAD_JIFFY_IOWAIT,
      THREAD_JIFFY_IRQ,
      THREAD_JIFFY_SOFTIRQ,
      THREAD_JIFFY_STEALTIME,
      THREAD_JIFFY_VIRTUALQUEST,
      THREAD_JIFFY_LAST,
   };



//////////////////////
// CLASS ThreadList //
//////////////////////
class ThreadList
{
/////////////
   public: //
/////////////

   // Const/dest:
   ThreadList();
   ThreadList(pid_t pid, pid_t tid);
   ~ThreadList();

   // Get/set:
   // @warning should probably be synchronized
   inline pid_t getPid()
   { return pid; }
   inline pid_t getTid()
   { return tid; }
   inline jthread getJThread()
   { return javaThread; }
   inline char *getName()
   { return name; }
   inline float getCpuUsage()
   { return cpuUsage; }
   inline float getCpuUsageRelative()
   { return cpuUsageRelative; }

   inline void setJThread(jthread _jthread)
   { javaThread = _jthread; }
   inline void setName(char *name)
   { if (strlen(name) < MAX_THREADNAMELENGTH) strcpy(this->name, name); }
   inline void setStatus(int status)
   { this->status = status; }
   inline int getStatus()
   { return status; }
   
   inline void setDaemon(bool d)
   { daemon = d; }
   inline bool isDaemon()
   { return daemon; }   

   // Management:
   static void clear();
   static unsigned int getNumberOf();
   static ThreadList *findByTid(pid_t _tid);
   static ThreadList *findByName(char *name);
   static void update();
   static void notify(JNIEnv* jni_env);
   static float getCpuUsageByTid(pid_t _tid);
   static float getCpuUsageRelativeByTid(pid_t _tid);

   // Stat manager:
   static bool updateStats();

   // Log:
   static void initLog();
   static void closeLog();


//////////////
   private: //
//////////////

   // Thread data:
   pid_t pid;
   pid_t tid;
   jthread javaThread;
   char name[MAX_THREADNAMELENGTH];
   char filename[MAX_THREADFILENAMELENGTH];
   int status;
   bool daemon;   

   // Performance stats:
   int file;
   unsigned long jiffies;
   float cpuUsage;
   float cpuUsageRelative;
   char buffer[MAX_THREADSTATBUFFERSIZE];

   // Log:
   static FILE *log;

   // Linked list:
   static ThreadList *firstElement;
   ThreadList *prev, *next;
   static pthread_mutex_t mutex;

   // Reference stats:
   static unsigned long totalJiffies;
   static unsigned long totalJiffiesRelative;
};

#endif // THREADLIST_H_INCLUDED
