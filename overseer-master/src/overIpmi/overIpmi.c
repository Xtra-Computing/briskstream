/**
 * @file    overIpmi.cpp
 * @brief	overIpmi hardware sensors reporter for Java
 *
 * @author	Achille Peternier and Daniele Bonetta (C) USI 2010, achille.peternier@gmail.com
 */



//////////////
// #INCLUDE //
//////////////
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <ipmi_monitoring.h>
#include "overIpmi.h"



////////////
// STATIC //
////////////

   // Config:
   struct ipmi_monitoring_ipmi_config config;

   // IPMI context:
   static ipmi_monitoring_ctx_t ctx = NULL;

   // Limits:
   static int maxNumberOfSensors = 0;

   // Lock:
   static char callbackPending = 0;
   static double dblResult = 0.0;
   static char strResult[256];
   static pthread_mutex_t mutex;



//////////////
// CALLBACK //
//////////////

/**
 * Callback invoked by ipmimonitoring to process sensors enumerated.
 */
static int ipmimonitoring_callback (ipmi_monitoring_ctx_t c, void *callback_data)
{
   void *data = ipmi_monitoring_read_sensor_reading(c);
   if (data == NULL)
   {
      callbackPending = 0;
      return 0;
   }

   strcpy(strResult, ipmi_monitoring_read_sensor_name(c));
   dblResult = *((double *) data);
   callbackPending = 0;
   return 0;
}

/**
 * Initialization method.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverIpmi_init(JNIEnv *env, jobject obj)
{
   // Build IPMI config:
   memset(&config, 0, sizeof(config));
   config.driver_type = -1;
   config.disable_auto_probe = 0;
   config.protocol_version = IPMI_MONITORING_PROTOCOL_VERSION_2_0;
   config.username = NULL;
   config.password = NULL;
   config.k_g = NULL;
   config.k_g_len = 0;
   // config.privilege_level = IPMI_MONITORING_PRIVILEGE_LEVEL_ADMIN;
   config.authentication_type = -1;
   config.cipher_suite_id = -1;
   config.session_timeout_len = -1;
   config.workaround_flags = 0;

   // Init IPMI:
   int errcode = ipmi_monitoring_init(0, NULL);
   if (errcode)
      return JNI_FALSE;

   // Init context:
   ctx = ipmi_monitoring_ctx_create();
   if (ctx == NULL)
      return JNI_FALSE;

   // Init mutex:
   pthread_mutex_init(&mutex, NULL);

   // Init callback:
   errcode = ipmi_monitoring_sensor_readings_by_record_id (ctx,
                                                          "127.0.0.1",
                                                          &config,
                                                          /*IPMI_MONITORING_SENSOR_READING_FLAGS_INTERPRET_OEM_DATA |
                                                          IPMI_MONITORING_SENSOR_READING_FLAGS_SHARED_SENSORS |
                                                          IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_NON_INTERPRETABLE_SENSORS |
                                                          IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_UNREADABLE_SENSORS*/
                                                          0,
                                                          0,
                                                          0,
                                                          NULL,
                                                          NULL);

   // Check:
   if (errcode < 0)
   {
      ipmi_monitoring_ctx_destroy(ctx);
      return JNI_FALSE;
   }

   // Done:
   maxNumberOfSensors = errcode;
   return JNI_TRUE;
}

/**
 * Gets number of available sensors.
 * @return number of sensors
 */
JNIEXPORT jint JNICALL Java_ch_usi_overseer_OverIpmi_getNumberOfSensors(JNIEnv *env, jobject obj)
{ return maxNumberOfSensors; }

/**
 * DeInitialization method.
 */
JNIEXPORT jboolean JNICALL Java_ch_usi_overseer_OverIpmi_free(JNIEnv *env, jobject obj)
{
   // Release context:
   if (ctx)
   {
      ipmi_monitoring_ctx_destroy(ctx);
      ctx = NULL;
   }

   // Done:
   return JNI_TRUE;
}

/**
 * Returns the value of a sensor, given its id.
 * @return sensor value, 0.0 if error
 */
JNIEXPORT jdouble JNICALL Java_ch_usi_overseer_OverIpmi_getSensorValue(JNIEnv *env, jobject obj, jint sensorId)
{
   // Safety net:
   if (sensorId < 0 || sensorId > maxNumberOfSensors)
      return 0.0;

   // Serialize requests:
   pthread_mutex_lock(&mutex);
   callbackPending = 1;
   dblResult = 0.0;

   // Very rude way to avoid the callback:
   static unsigned int list[1];
   list[0] = sensorId;

   // Init callback:
   int errcode = ipmi_monitoring_sensor_readings_by_record_id(ctx,
                                                              "127.0.0.1",
                                                              &config,
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_INTERPRET_OEM_DATA |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_SHARED_SENSORS |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_NON_INTERPRETABLE_SENSORS |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_UNREADABLE_SENSORS
                                                              0,
                                                              list,
                                                              1,
                                                              ipmimonitoring_callback,
                                                              NULL);

   // Check:
   if (errcode <= 0)
   {
      pthread_mutex_unlock(&mutex);
      return 0.0;
   }

   // Wait:
   while (callbackPending);

   // Invoke callback, wait for result:
   double sensorValue = dblResult;

   // Done:
   pthread_mutex_unlock(&mutex);
   return sensorValue;
}

/**
 * Returns the name of a sensor, given its id.
 * @return sensor name
 */
JNIEXPORT jstring JNICALL Java_ch_usi_overseer_OverIpmi_getSensorName(JNIEnv *env, jobject obj, jint sensorId)
{
   // Safety net:
   if (sensorId < 0 || sensorId > maxNumberOfSensors)
      return NULL;

   // Serialize requests:
   pthread_mutex_lock(&mutex);
   callbackPending = 1;
   strcpy(strResult, "[none]");

   // Very rude way to avoid the callback:
   static unsigned int list[1];
   list[0] = sensorId;

   // Init callback:
   int errcode = ipmi_monitoring_sensor_readings_by_record_id(ctx,
                                                              "127.0.0.1",
                                                              &config,
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_INTERPRET_OEM_DATA |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_SHARED_SENSORS |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_NON_INTERPRETABLE_SENSORS |
                                                              //IPMI_MONITORING_SENSOR_READING_FLAGS_IGNORE_UNREADABLE_SENSORS
                                                              0,
                                                              list,
                                                              1,
                                                              ipmimonitoring_callback,
                                                              NULL);

   // Check:
   if (errcode <= 0)
   {
      pthread_mutex_unlock(&mutex);
      return NULL;
   }

   // Wait:
   while (callbackPending);

   // Invoke callback, wait for result:
   jstring sensorName = (*env)->NewStringUTF(env, strResult);

   // Done:
   pthread_mutex_unlock(&mutex);
   return sensorName;
}
