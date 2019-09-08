/**
 * @file    smartLock.h
 * @brief   Handy mutex manager class
 *
 * @author	Achille Peternier (C) USI 2010, achille.peternier@gmail.com
 */
#ifndef SMARTLOCK_H_INCLUDED
#define SMARTLOCK_H_INCLUDED



//////////////
// #INCLUDE //
//////////////
#include <pthread.h>
#include <stdio.h>


/////////////
// #DEFINE //
/////////////

   // Macros for locking:
   #define MUTEX_SYNC(x)   SmartLock smartlock(&x)
   //#define MUTEX_SYNC(x)
   #define MUTEX_BEGIN(x)  do { SmartLock smartlock(&x)
   #define MUTEX_END(x)    } while(0)



/////////////////////
// CLASS SmartLock //
/////////////////////
class SmartLock
{
/////////////
   public: //
/////////////

   // Const/dest:
   SmartLock(pthread_mutex_t *m) : mutex(m)
   { pthread_mutex_lock(mutex); }

   ~SmartLock()
   { pthread_mutex_unlock(mutex); }


//////////////
   private: //
//////////////

   // Mutex reference:
   pthread_mutex_t *mutex;
};

#endif // SMARTLOCK_H_INCLUDED
