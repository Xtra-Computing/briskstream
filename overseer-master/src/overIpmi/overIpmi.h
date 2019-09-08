#ifndef OVERIPMI_H_INCLUDED
#define OVERIPMI_H_INCLUDED



//////////////
// #INCLUDE //
//////////////
#include <jni.h>



/////////////
// #DEFINE //
/////////////

    // Generic:
    #define OI_NAME     "overIpmi v0.1a"
    #define OI_VERSION  0.1



/////////////
// METHODS //
/////////////

JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverIpmi_init(JNIEnv *env, jobject obj);
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverIpmi_free(JNIEnv *env, jobject obj);
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverIpmi_getNumberOfSensors(JNIEnv *env, jobject obj);
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverIpmi_getSensorName(JNIEnv *env, jobject obj, jint sensorId);
JNIEXPORT jdouble JNICALL Java_ch_usi_overseer_OverIpmi_getSensorValue(JNIEnv *env, jobject obj, jint sensorId);

#endif // OVERIPMI_H_INCLUDED
