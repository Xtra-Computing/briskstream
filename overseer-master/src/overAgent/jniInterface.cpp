////////////////////////////////////////////////////
//                                                //
// overAgent v0.1, Achille Peternier (C) 2010 USI //
//                                                //
////////////////////////////////////////////////////



//////////////
// #INCLUDE //
//////////////
#include "ch_usi_overseerJNI_WrapperJNI.h"
#include "overAgent.h"
#include "threadList.h"
#include <stdio.h>



////////////
// GLOBAL //
////////////

   // From "overAgent.cpp":
   extern GlobalAgentData gdata;



/////////////
// METHODS //
/////////////

JNIEXPORT jfloat JNICALL Java_ch_usi_overseer_OverAgent_getThreadCpuUsage(JNIEnv *env, jobject obj, jint threadId)
{ return (jfloat) ThreadList::getCpuUsageByTid(threadId); }

JNIEXPORT jfloat JNICALL Java_ch_usi_overseer_OverAgent_getThreadCpuUsageRelative(JNIEnv *env, jobject obj, jint threadId)
{ return (jfloat) ThreadList::getCpuUsageRelativeByTid(threadId); }

JNIEXPORT void JNICALL Java_ch_usi_overseer_OverAgent_updateStats(JNIEnv *env, jobject obj)
{ ThreadList::updateStats(); }

JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverAgent_getVersion(JNIEnv *env, jobject obj)
{ return (jint) (OA_VERSION * 10.0); }

JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverAgent_getNumberOfThreads(JNIEnv *env, jobject obj)
{ return ThreadList::getNumberOf(); }

JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverAgent_isRunning(JNIEnv *env, jobject obj)
{ return gdata.agent_is_running; }

JNIEXPORT void JNICALL Java_ch_usi_overseer_OverAgent_init(JNIEnv *env, jobject obj)
{
   // Find static class:
   gdata.overAgentClass = env->FindClass("ch/usi/overseer/OverAgent");
   if (gdata.overAgentClass == NULL)
   {
      printf("Class not found\n");
      return;
   }

   // Retrieve event codes from Java:
   jfieldID fid = env->GetStaticFieldID(gdata.overAgentClass, "THREAD_CREATED", "I");
   gdata.THREAD_CREATED = env->GetStaticIntField(gdata.overAgentClass, fid);
   fid = env->GetStaticFieldID(gdata.overAgentClass, "THREAD_TERMINATED", "I");
   gdata.THREAD_TERMINATED = env->GetStaticIntField(gdata.overAgentClass, fid);

   // Get Java callback:
   gdata.callbackJava = env->GetStaticMethodID(gdata.overAgentClass, "eventCallback", "(IILjava/lang/String;)V");
   if (gdata.callbackJava == NULL)
   {
      printf("Method not found\n");
      return;
   }
   gdata.interface_is_ready = true;
   ThreadList::notify(env);
}
