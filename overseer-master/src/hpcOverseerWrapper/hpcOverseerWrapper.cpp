#include "ch_usi_overseerJNI_WrapperJNI.h"
#include "hpcOverseer.h"

// Static flags:
static bool running = false;

/**
 * Wrapper for getting version number
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getVersion(JNIEnv *env, jobject obj)
{
    return (jint) (HPCO_VERSION * 10.0);
}

/**
 * Wrapper for method HpcOverseer::init.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_init(JNIEnv *, jobject)
{
    // Safety net:
    if (running)
        return false;

    // Init:
    running = HpcOverseer::initialize();

    // Done:
    return running;
}

/**
 * Wrapper for method HpcOverseer::terminate.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_terminate(JNIEnv *, jobject)
{
    if (!running)
        return false;

    HpcOverseer::terminate();

    // Done:
    running = false;
    return true;
}

/**
 * Wrapper for HpcOverseer::getNumberOfCPUs
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfCpus(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfCpus();
}

/**
 * Wrapper for HpcOverseer::getNumberOfNumas
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfNumas(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfNumas();
}

/**
 * Wrapper for HpcOverseer::getNumberOfCpusPerNuma
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfCpusPerNuma(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfCpusPerNuma();
}

/**
 * Wrapper for HpcOverseer::getNumberOfPus
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfPus(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfPus();
}

/**
 * Wrapper for HpcOverseer::getNumberOfCoresPerCPU
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfCoresPerCpu(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfCoresPerCpu();
}

/**
 * Wrapper for HpcOverseer::getCpuCoreMapping
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getCpuCoreMapping(JNIEnv *, jobject, jint cpu, jint core)
{
    return HpcOverseer::getCpuCoreMapping(cpu, core);
}

/**
 * Wrapper for method HpcOverseer::initEvents.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_initEvents(JNIEnv *env, jobject, jstring params)
{
    // Access vars:
    jboolean iscopy;
    const char *paramString = env->GetStringUTFChars(params, &iscopy);

    // Init events:
    bool result = HpcOverseer::initEvents((char *)paramString);

    // Done:
    env->ReleaseStringUTFChars(params, paramString);
    return result;
}

/**
 * Wrapper for HpcOverseer::bindEventsToCore.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_bindEventsToCore(JNIEnv *, jobject, jint coreId)
{
    // Access vars:
    // ...

    // Bind events:
    bool result = HpcOverseer::bindEventsToCore(coreId);

   // Done:
   return result;
}

/**
 * Wrapper for HpcOverseer::bindEventsToThread.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_bindEventsToThread(JNIEnv *, jobject, jint threadId)
{
    // Access vars:
    // ...

    // Bind events:
    bool result = HpcOverseer::bindEventsToThread(threadId);

   // Done:
   return result;
}

/**
 * Wrapper for HpcOverseer::start.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_start(JNIEnv *, jobject)
{
    return HpcOverseer::start();
}

/**
 * Wrapper for HpcOverseer::stop.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_stop(JNIEnv *, jobject)
{
    return HpcOverseer::stop();
}

/**
 * Wrapper for HpcOverseer::getEventsFromCPU.
 */
JNIEXPORT jlong JNICALL Java_ch_usi_overseer_OverHpc_getEventFromCore(JNIEnv *, jobject, jint coreId, jint event)
{ return HpcOverseer::getEventFromCore(coreId, event); }
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfEventsFromCore(JNIEnv *, jobject, jint coreId)
{ return HpcOverseer::getNumberOfEventsFromCore(coreId); }
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverHpc_getEventNameFromCore(JNIEnv *env, jobject, jint coreId, jint event)
{ return env->NewStringUTF(HpcOverseer::getEventNameFromCore(coreId, event)); }

/**
 * Wrapper for HpcOverseer::getEventsFromThread.
 */
JNIEXPORT jlong JNICALL Java_ch_usi_overseer_OverHpc_getEventFromThread(JNIEnv *, jobject, jint threadId, jint event)
{ return HpcOverseer::getEventFromThread(threadId, event); }
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfEventsFromThread(JNIEnv *, jobject, jint threadId)
{ return HpcOverseer::getNumberOfEventsFromThread(threadId); }
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverHpc_getEventNameFromThread(JNIEnv *env, jobject, jint threadId, jint event)
{ return env->NewStringUTF(HpcOverseer::getEventNameFromThread(threadId, event)); }

/**
 * Wrapper for HpcOverseer::getNumberOfInitializedEvents.
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfInitializedEvents(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfInitializedEvents();
}

/** Wrapper for HpcOverseer::getNumberAvailableEvents
 *
 */
 JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfAvailableEvents(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfAvailableEvents();
}

/**
 * Wrapper for HpcOverseer::getNumberOfThreads.
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getNumberOfThreads(JNIEnv *, jobject)
{
    return HpcOverseer::getNumberOfThreads();
}

/**
 * Wrapper for HpcOverseer::getInitializedEventString.
 */
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverHpc_getInitializedEventString(JNIEnv *env, jobject, jint event)
{
    return env->NewStringUTF(HpcOverseer::getInitializedEventString(event));
}

/**
 * Wrapper for HpcOverseer::getAvailableEventsString.
 */
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverHpc_getAvailableEventsString(JNIEnv *env, jobject)
{
    return env->NewStringUTF(HpcOverseer::getAvailableEventsString());
}


/**
 * Wrapper for HpcOverseer::getThreadId.
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverHpc_getThreadId(JNIEnv *, jobject)
{
    return HpcOverseer::getThreadId();
}

/**
 * Wrapper for HpcO_SetThreadAffinity.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_setThreadAffinity(JNIEnv *, jobject, jint threadId, jlong mask)
{
    return HPCO_SetThreadAffinity(threadId, mask);
}

/**
 * Wrapper for HPCO_SetThreadAffinityMask.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_setThreadAffinityMask(JNIEnv *env, jobject, jint threadId, jintArray javaArray)
{
   int size = env->GetArrayLength(javaArray);
   int *maskArray = env->GetIntArrayElements(javaArray, NULL);

   // DEBUG:
   /*printf("Mask: ");
   for (int c=0; c<size; c++)
      if (maskArray[c]) printf("1");
      else printf("0");
   printf("\n");*/

   bool result = HPCO_SetThreadAffinityMask(threadId, maskArray, size);
   env->ReleaseIntArrayElements(javaArray, maskArray, 0);
   return result;
}


/**
 * Wrapper for HPCO_GetThreadAffinity.
 */
JNIEXPORT jlong JNICALL Java_ch_usi_overseer_OverHpc_getThreadAffinity(JNIEnv *, jobject, jint threadId)
{
    return HPCO_GetThreadAffinity(threadId);
}

/**
 * Wrapper for RdstcTimerEntry::reset
 */
JNIEXPORT void JNICALL Java_ch_usi_overseer_OverHpc_reset(JNIEnv *, jobject)
{
    RdtscTimerEntry::reset();
}

/**
 * Wrapper for RdstcTimerEntry::setEntry
 */
JNIEXPORT void JNICALL Java_ch_usi_overseer_OverHpc_setEntry(JNIEnv *, jobject, jint paramA, jint paramB, jint paramC)
{
    RdtscTimerEntry::setEntry(paramA, paramB, paramC);
}

/**
 * Wrapper for method RdtscTimerEntry::logToFile
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverHpc_logToFile(JNIEnv *env, jobject, jstring filename)
{
    // Access vars:
    jboolean iscopy;
    const char *filenameString = env->GetStringUTFChars(filename, &iscopy);

    // Init events:
    bool result = RdtscTimerEntry::logToFile(filenameString);

    // Done:
    env->ReleaseStringUTFChars(filename, filenameString);
    return result;
}
