#include <jni.h>
#include <syscall.h>
#include "GetThreadID.h"

JNIEXPORT jint JNICALL
Java_GetThreadID_get_1tid(JNIEnv *env, jobject obj) {
    jint tid = syscall(__NR_gettid);
    return tid;
}