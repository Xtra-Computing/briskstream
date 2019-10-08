package profilier;

import org.slf4j.Logger;

import static profilier.CONTROL.kMaxThreadNum;

public class MeasureTools {
    protected static Metrics metrics = Metrics.getInstance();

    static long[] timer1 = new long[kMaxThreadNum];//Obtain statistics on each thread.

    static long[] timer2 = new long[kMaxThreadNum];//Obtain statistics on each thread.

    static long[] timer3 = new long[kMaxThreadNum];//Obtain statistics on each thread.


    public static void BEGIN_TIME_MEASURE_NO_PAT(int thread_id) {

        if (CONTROL.enable_profile) {
            timer1[thread_id] = System.nanoTime();
            timer2[thread_id] = System.nanoTime();
            timer3[thread_id] = 0;
        }
    }


    public static void END_TIME_MEASURE_BUILD(int thread_id) {

        if (CONTROL.enable_profile) {
            timer2[thread_id] -= System.nanoTime();
            metrics.build_time[thread_id].addValue(timer2[thread_id]);
        }
    }

    public static void END_TIME_MEASURE_NO_PAT(int thread_id) {

        if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile) {
            timer1[thread_id] -= System.nanoTime();
            metrics.total[thread_id].addValue(timer1[thread_id]);
        }
    }

    public static void PRINT_TIMING_NO_PAT(Logger LOG, int thread_id, long result) {

        LOG.info(String.format("RUNTIME TOTAL (%.4f), BUILD (%.4f), PART (%d) (ns):", metrics.total[thread_id].getSum(), metrics.build_time[thread_id].getSum(), 0));
        LOG.info(String.format("TOTAL_TUPLES (%d), TIME_PER_TUPLE (%.4f)", result, metrics.total[thread_id].getSum() / result));

    }
}