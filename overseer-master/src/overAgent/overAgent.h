////////////////////////////////////////////////////
//                                                //
// overAgent v0.1, Achille Peternier (C) 2010 USI //
//                                                //
////////////////////////////////////////////////////
#ifndef OVERAGENT_H_INCLUDED
#define OVERAGENT_H_INCLUDED



//////////////
// #INCLUDE //
//////////////
#include <jvmti.h>



/////////////
// #DEFINE //
/////////////

    // Generic:
    #define OA_NAME     "overAgent v0.2a"
    #define OA_VERSION  0.2



/////////////
// STRUCTS //
/////////////

// Agent global vars:
typedef struct
{
   jvmtiEnv    *jvmti;
   jboolean    vm_is_started;
   jboolean    interface_is_ready;
   jboolean    agent_is_running;

   // Data access Lock:
   jrawMonitorID  lock;

   // Callback data:
   jclass overAgentClass;
   jmethodID callbackJava;

   // Event codes:
   jint THREAD_CREATED, THREAD_TERMINATED;
} GlobalAgentData;



/////////////
// METHODS //
/////////////

// Common:
void check_jvmti_error(jvmtiEnv *jvmti, jvmtiError errnum, const char *str);
void get_thread_name(jvmtiEnv *jvmti, jthread thread, char *tname, int maxlen);
bool is_thread_daemon(jvmtiEnv *jvmti, jthread thread);

#endif // OVERAGENT_H_INCLUDED
