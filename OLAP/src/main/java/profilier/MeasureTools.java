package profilier;

import static profilier.CONTROL.kMaxThreadNum;

public class MeasureTools {
    protected static Metrics metrics = Metrics.getInstance();
    static long[] total_start = new long[kMaxThreadNum];//Obtain statistics on each thread.
    static long[] total_end = new long[kMaxThreadNum];//Obtain statistics on each thread.

    static long[] timer1 = new long[kMaxThreadNum];//Obtain statistics on each thread.

    static long[] timer2 = new long[kMaxThreadNum];//Obtain statistics on each thread.

    static long[] timer3 = new long[kMaxThreadNum];//Obtain statistics on each thread.


    public static void BEGIN_TIME_MEASURE_NO_PAT(int thread_id) {

        if (CONTROL.enable_profile) {
            total_start[thread_id] = System.nanoTime();
            timer1[thread_id] = System.nanoTime();
            timer2[thread_id] = System.nanoTime();
            timer3[thread_id] = 0;
        }
    }


    public static void END_TIME_MEASURE_BUILD(int thread_id) {

        if (CONTROL.enable_profile) {
            timer2[thread_id] -= System.nanoTime();
        }

    }

    public static void END_TIME_MEASURE(int thread_id) {

        if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile) {

            total_end[thread_id] = System.nanoTime();

            metrics.total[thread_id].addValue((total_start[thread_id] - total_end[thread_id]));

        }
    }

}