/**
 * @file    overAgent.cpp
 * @brief	overAgent HPC profiler for Java
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include "overAgent.h"
#include "threadList.h"
#include "ch_usi_overseerJNI_WrapperJNI.h"
#include <unistd.h>
#include <syscall.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jvmti.h>



////////////
// GLOBAL //
////////////

   // Global global:
   GlobalAgentData gdata;



////////////
// STATIC //
////////////

   // Local global:
   static jvmtiEnv *jvmti = NULL;
   static jvmtiCapabilities capa;



////////////
// COMMON //
////////////

/**
 * Error checking procedure.
 */
void check_jvmti_error(jvmtiEnv *jvmti, jvmtiError errnum, const char *str)
{
    if ( errnum != JVMTI_ERROR_NONE )
    {
        char *errnum_str;
        errnum_str = NULL;

        jvmti->GetErrorName(errnum, &errnum_str);
        printf("ERROR: JVMTI: %d(%s): %s\n", errnum, (errnum_str==NULL?"Unknown":errnum_str), (str==NULL?"":str));
    }
}

/**
 * Get a thread name.
 */
void get_thread_name(jvmtiEnv *jvmti, jthread thread, char *tname, int maxlen)
{
    jvmtiThreadInfo info;
    jvmtiError      error;

    // Reset trash:
    memset(&info, 0, sizeof(info));
    strcpy(tname, "[unknown]");

    // Available only after VMInit event:
    if (gdata.vm_is_started == JNI_FALSE)
        return;

    // Get the thread information, which includes the name:
    error = jvmti->GetThreadInfo(thread, &info);
    check_jvmti_error(jvmti, error, "Cannot get thread info");

    // The thread might not have a name, be careful here:
    if (info.name != NULL)
    {
        int len;

        // Copy the thread name into tname if it will fit
        len = (int)strlen(info.name);
        if ( len < maxlen ) {
            (void)strcpy(tname, info.name);
        }

       // Every string allocated by JVMTI needs to be freed
	    error = jvmti->Deallocate((unsigned char *) info.name);
       check_jvmti_error(jvmti, error, "Cannot release thread info");
    }
}


/**
 * Get thread daemon flag:
 */
bool is_thread_daemon(jvmtiEnv *jvmti, jthread thread)
{
    jvmtiThreadInfo info;
    jvmtiError      error;

    // Reset trash:
    memset(&info, 0, sizeof(info));
    bool isDaemon = false;

    // Available only after VMInit event:
    if (gdata.vm_is_started == JNI_FALSE)
        return isDaemon;

    // Get the thread information, which includes the flag:
    error = jvmti->GetThreadInfo(thread, &info);
    check_jvmti_error(jvmti, error, "Cannot get thread info");

    // Copy the thread name into tname if it will fit
    isDaemon = info.is_daemon;

    if (info.name != NULL)
    {
    	// Every string allocated by JVMTI needs to be freed
	error = jvmti->Deallocate((unsigned char *) info.name);
        check_jvmti_error(jvmti, error, "Cannot release thread info");
    }

    // Done:
    return isDaemon;
}



//////////
// SYNC //
//////////

static void enter_critical_section()
{
    jvmtiError error;
    error = jvmti->RawMonitorEnter(gdata.lock);
    check_jvmti_error(jvmti, error, "Cannot enter with raw monitor");
}

static void exit_critical_section()
{
    jvmtiError error;

    error = jvmti->RawMonitorExit(gdata.lock);
    check_jvmti_error(jvmti, error, "Cannot exit with raw monitor");
}



///////////////
// CALLBACKS //
///////////////

/**
 * VM init callback
 */
static void JNICALL callbackVMInit(jvmtiEnv *jvmti_env, JNIEnv* jni_env, jthread thread)
{
   enter_critical_section();
   {
      // VM up, dispatch pending notifications:
      gdata.vm_is_started = JNI_TRUE;
      ThreadList::update();
   }
   exit_critical_section();
}

/**
 * VM Death callback
 */
static void JNICALL callbackVMDeath(jvmtiEnv *jvmti_env, JNIEnv* jni_env)
{
   enter_critical_section();
   {
      // WM down, dispatch pending notifications:
      gdata.vm_is_started = JNI_FALSE;
      ThreadList::notify(jni_env);
      ThreadList::clear();
   }
   exit_critical_section();
}

/**
 * Thread start
 */
void JNICALL callbackThreadStart(jvmtiEnv *jvmti, JNIEnv* jni_env, jthread thread)
{
   enter_critical_section();
   {
      char name[MAX_THREADNAMELENGTH];
      pid_t pid = syscall(__NR_getpid);
      pid_t tid = syscall(__NR_gettid);
      get_thread_name(jvmti, thread, name, MAX_THREADFILENAMELENGTH - 1);

      ThreadList *_thread = new ThreadList(pid, tid);

      _thread->setJThread(thread);     
      _thread->setName(name);
      _thread->setDaemon(is_thread_daemon(jvmti, thread));

      // Send notification:
      if (gdata.vm_is_started && gdata.interface_is_ready)
      {
         jstring callbackArg = jni_env->NewStringUTF(name);
         jni_env->CallStaticVoidMethod(gdata.overAgentClass, gdata.callbackJava, gdata.THREAD_CREATED, (jint) tid, callbackArg);
         _thread->setStatus(THREAD_STATUS_CREATIONNOTIFIED);
      }
   }
   exit_critical_section();
}

/**
 * Thread end
 */
static void JNICALL callbackThreadEnd(jvmtiEnv *jvmti, JNIEnv* jni_env, jthread thread)
{
   enter_critical_section();
   {
      // Send notification:
      if (gdata.vm_is_started && gdata.interface_is_ready)
      {
         char name[MAX_THREADNAMELENGTH];
         pid_t tid = syscall(__NR_gettid);
         ThreadList *_thread = ThreadList::findByTid(tid);

         // Should never happen, but on J9...:
         if (_thread == NULL)
         {
            printf("[!] Agent found an unnotified event termination\n");
            goto abort;
         }

         strcpy(name, _thread->getName());
         jstring callbackArg = jni_env->NewStringUTF(name);
         jni_env->CallStaticVoidMethod(gdata.overAgentClass, gdata.callbackJava, gdata.THREAD_TERMINATED, (jint) tid, callbackArg);
         _thread->setStatus(THREAD_STATUS_TERMINATIONNOTIFIED);
         delete _thread;
      }
   }
abort:
   exit_critical_section();
}



/////////////
// METHODS //
/////////////

/**
 * Agent entry point
 */
JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *jvm, char *options, void *reserved)
{
    // Credits:
    // printf("%s, Achille Peternier (C) 2010 USI\n\n", OA_NAME);

    jvmtiError error;
    jint res;
    jvmtiEventCallbacks callbacks;


    //////////////////////////
    // Init JVMTI environment:
    res = jvm->GetEnv((void **) &jvmti, JVMTI_VERSION_1_0);
    if (res != JNI_OK || jvmti == NULL)
    {
	    printf("ERROR: Unable to access JVMTI Version 1 (0x%x),"
                " is your J2SE a 1.5 or newer version?"
                " JNIEnv's GetEnv() returned %d\n",
               JVMTI_VERSION_1, res);
    }

    // Globalize it for Agent_OnUnload():
    gdata.jvmti = jvmti;


    /////////////////////
    // Init capabilities:
    memset(&capa, 0, sizeof(jvmtiCapabilities));
    capa.can_signal_thread = 1;
    capa.can_get_owned_monitor_info = 1;

    error = jvmti->AddCapabilities(&capa);
    check_jvmti_error(jvmti, error, "Unable to get necessary JVMTI capabilities.");


    //////////////////
    // Init callbacks:
    memset(&callbacks, 0, sizeof(callbacks));
    callbacks.VMInit = &callbackVMInit;
    callbacks.VMDeath = &callbackVMDeath;
    callbacks.ThreadEnd = &callbackThreadEnd;
    callbacks.ThreadStart = &callbackThreadStart;

    error = jvmti->SetEventCallbacks(&callbacks, (jint)sizeof(callbacks));
    check_jvmti_error(jvmti, error, "Cannot set jvmti callbacks");


    ///////////////
    // Init events:
    error = jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_INIT, (jthread)NULL);
    error = jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_VM_DEATH, (jthread)NULL);
    error = jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_THREAD_START, (jthread)NULL);
    error = jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_THREAD_END, (jthread)NULL);
    check_jvmti_error(jvmti, error, "Cannot set event notification");

    // Here we create a raw monitor for our use in this agent to
    // protect critical sections of code:
    error = gdata.jvmti->CreateRawMonitor("agent data", &(gdata.lock));
    check_jvmti_error(jvmti, error, "Cannot create raw monitor");

    // Reset /proc/stat values:
    ThreadList::updateStats();

    // Start logging:
    ThreadList::initLog();

    // Done:
    gdata.agent_is_running = true;
    return JNI_OK;
}

/**
 * Agent exit point. Last code executed.
 */
JNIEXPORT void JNICALL Agent_OnUnload(JavaVM *vm)
{
   // Conclude:
   // printf("%s quit\n", OA_NAME);
   ThreadList::closeLog();
}


