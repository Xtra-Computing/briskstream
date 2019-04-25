package engine.profiler;

import applications.CONTROL;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import static engine.Meta.MetaTypes.kMaxThreadNum;

public class Metrics {

    private static Metrics ourInstance = new Metrics();
    public static int NUM_ACCESSES = 16;//10 as default setting. 2 for short transaction, 10 for long transaction.? --> this is the setting used in YingJun's work. 16 is the default value_list used in 1000core machine.
    public static int NUM_ITEMS = 1_000_000;//1. 1_000_000; 2. ? ; 3. 1_000  //1_000_000 YCSB has 16 million records, Ledger use 200 million records.
    public static int H2_SIZE;
    public DescriptiveStatistics[] exe_time = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.

//    public volatile boolean measure = false;

    //    public void start_measurement() {
//        measure = true;
//    }
    public DescriptiveStatistics[] useful_time = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
    public DescriptiveStatistics[] average_tp_event = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per event thread without considering synchronization.
    public DescriptiveStatistics[] abort_time = new DescriptiveStatistics[kMaxThreadNum];//abort


    // Op id, descriptive
//    public Map<String, DescriptiveStatistics> useful_time = new HashMap<>();//useful_work time.
//    public Map<String, DescriptiveStatistics> abort_time = new HashMap<>();//abort
//    public Map<String, DescriptiveStatistics> ts_allocation = new HashMap<>();//timestamp allocation
//    public Map<String, DescriptiveStatistics> index_time = new HashMap<>();//index
//    public Map<String, DescriptiveStatistics> wait = new HashMap<>();// wait lock and order.
//    public Map<String, DescriptiveStatistics> exe_time = new HashMap<>();//not in use.
//    public Map<String, DescriptiveStatistics> order_wait = new HashMap<>();//order wait
//    public Map<String, DescriptiveStatistics> enqueue_time = new HashMap<>();//event enqueue

    //TODO: single op for now. per task/thread.
    public DescriptiveStatistics[] ts_allocation = new DescriptiveStatistics[kMaxThreadNum];//timestamp allocation
    public DescriptiveStatistics[] index_time = new DescriptiveStatistics[kMaxThreadNum];//index
    public DescriptiveStatistics[] wait = new DescriptiveStatistics[kMaxThreadNum];// wait lock and order.
    public DescriptiveStatistics[] lock = new DescriptiveStatistics[kMaxThreadNum];// wait lock and order.
    public DescriptiveStatistics[] average_tp = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread without considering synchronization.
    public DescriptiveStatistics[] average_tp_submit = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread without considering synchronization.
    public DescriptiveStatistics[] average_tp_w_syn = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread with synchronization.
//    public Map<Integer, DescriptiveStatistics> exe_time = new HashMap<>();//not in use.
    /**
     * Specially for T-Stream..
     */

    public DescriptiveStatistics[] enqueue_time = new DescriptiveStatistics[kMaxThreadNum];//event enqueue

    private Metrics() {
    }

    public static Metrics getInstance() {
        return ourInstance;
    }

    /**
     * Initilize all metric counters.
     */
    public void initilize(String ID, int num_access) {
//        exe_time.put(ID, new DescriptiveStatistics());
//        useful_time.put(ID, new DescriptiveStatistics());
//        abort_time.put(ID, new DescriptiveStatistics());
//        index_time.put(ID, new DescriptiveStatistics());
//        wait.put(ID, new DescriptiveStatistics());
//        ts_allocation.put(ID, new DescriptiveStatistics());
//        enqueue_time.put(ID, new DescriptiveStatistics());

        NUM_ACCESSES = num_access;
    }

    public void initilize(int task) {
//        average_tp.put(task, new DescriptiveStatistics());
//        average_tp_w_syn.put(task, new DescriptiveStatistics());
//
//        useful_time.put(task, new DescriptiveStatistics());
//        abort_time.put(task, new DescriptiveStatistics());
//
//        ts_allocation.put(task, new DescriptiveStatistics());
//        index_time.put(task, new DescriptiveStatistics());
//
//        wait.put(task, new DescriptiveStatistics());
//        exe_time.put(task, new DescriptiveStatistics());

        useful_time[task] = new DescriptiveStatistics();
        exe_time[task] = new DescriptiveStatistics();
        abort_time[task] = new DescriptiveStatistics();
        ts_allocation[task] = new DescriptiveStatistics();
        index_time[task] = new DescriptiveStatistics();
        wait[task] = new DescriptiveStatistics();
        lock[task] = new DescriptiveStatistics();
        average_tp[task] = new DescriptiveStatistics();
        average_tp_submit[task] = new DescriptiveStatistics();
        average_tp_event[task] = new DescriptiveStatistics();
        average_tp_w_syn[task] = new DescriptiveStatistics();
        enqueue_time[task] = new DescriptiveStatistics();
    }


    public static class MeasureTools {
        protected static Metrics metrics = getInstance();
        static long[] txn_start = new long[kMaxThreadNum];
        static double[] txn_total = new double[kMaxThreadNum];
        static long[] txn_wait_start = new long[kMaxThreadNum];
        static long[] txn_wait = new long[kMaxThreadNum];
        static long[] txn_lock_start = new long[kMaxThreadNum];
        static long[] txn_lock = new long[kMaxThreadNum];
        static long[] prepare_start = new long[kMaxThreadNum];
        static long[] prepare_time = new long[kMaxThreadNum];
        static long[] compute_start = new long[kMaxThreadNum];
        static long[] compute_end = new long[kMaxThreadNum];
        static double[] compute_total = new double[kMaxThreadNum];
        static long[] index_start = new long[kMaxThreadNum];
        static long[] index_time = new long[kMaxThreadNum];
        static long[] abort_start = new long[kMaxThreadNum];
        static long[] abort_time = new long[kMaxThreadNum];
        static long[] ts_allocate_start = new long[kMaxThreadNum];
        static long[] ts_allocate = new long[kMaxThreadNum];
        //t-stream special.
        static long[] read_handle_start = new long[kMaxThreadNum];
        static long[] read_handle = new long[kMaxThreadNum];
        static long[] write_handle_start = new long[kMaxThreadNum];
        static long[] write_handle = new long[kMaxThreadNum];
        static long[] tp_core_start = new long[kMaxThreadNum];
        static long[] tp_core = new long[kMaxThreadNum];
        static long[] tp_submit_start = new long[kMaxThreadNum];
        static long[] tp_submit = new long[kMaxThreadNum];
        static long[] tp_core_event = new long[kMaxThreadNum];
        static long[] tp_start = new long[kMaxThreadNum];
        static long[] tp = new long[kMaxThreadNum];

        static long[] measure_counts = new long[kMaxThreadNum];

        public static void BEGIN_TRANSACTION_TIME_MEASURE(int thread_id) {

            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_start[thread_id] = System.nanoTime();
        }

        public static void BEGIN_PREPARE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                prepare_start[thread_id] = System.nanoTime();
        }

        public static void END_PREPARE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound) {
                prepare_time[thread_id] = System.nanoTime() - prepare_start[thread_id];
            }
        }

        public static void END_PREPARE_TIME_MEASURE_TS(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound) {
                prepare_time[thread_id] += System.nanoTime() - prepare_start[thread_id];
            }
        }

        public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_lock_start[thread_id] = System.nanoTime();
        }

        public static void END_LOCK_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_lock[thread_id] = (System.nanoTime() - txn_lock_start[thread_id]);
        }

        public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_wait_start[thread_id] = System.nanoTime();
        }

        public static void END_WAIT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_wait[thread_id] = (System.nanoTime() - txn_wait_start[thread_id] - txn_lock[thread_id]);
        }

        public static void BEGIN_COMPUTE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                compute_start[thread_id] = System.nanoTime();
        }

        public static void END_COMPUTE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound) {
                compute_total[thread_id] = System.nanoTime() - compute_start[thread_id];
            }
        }

        //needs to include write compute time also for TS.
        public static void END_COMPUTE_TIME_MEASURE_TS(int thread_id, double write_useful_time, int size) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                compute_total[thread_id] = System.nanoTime() - compute_start[thread_id] + (write_useful_time * size);
        }

        public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                index_start[thread_id] = System.nanoTime();
        }

        public static void END_INDEX_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                index_time[thread_id] += System.nanoTime() - index_start[thread_id];
        }

        public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_start[thread_id] = System.nanoTime();
        }

        public static void END_ABORT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_time[thread_id] = System.nanoTime() - abort_start[thread_id];
        }

        public static void CLEAN_ABORT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_time[thread_id] = 0;
        }

        public static void BEGIN_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                ts_allocate_start[thread_id] = System.nanoTime();
        }

        public static void END_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                ts_allocate[thread_id] = System.nanoTime() - ts_allocate_start[thread_id];
        }

        //t-stream special

        public static void BEGIN_READ_HANDLE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                read_handle_start[thread_id] = System.nanoTime();
        }

        public static void END_READ_HANDLE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                read_handle[thread_id] += System.nanoTime() - read_handle_start[thread_id];
        }

        public static void BEGIN_WRITE_HANDLE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                write_handle_start[thread_id] = System.nanoTime();
        }

        public static void END_WRITE_HANDLE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                write_handle[thread_id] += System.nanoTime() - write_handle_start[thread_id];
        }


        public static void BEGIN_TP_SUBMIT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_submit_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_SUBMIT_TIME_MEASURE(int thread_id, int size) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound) {
                tp_submit[thread_id] = System.nanoTime() - tp_submit_start[thread_id];
            }
        }


        public static void BEGIN_TP_CORE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_core_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_CORE_TIME_MEASURE(int thread_id, int size) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound) {
                tp_core[thread_id] = System.nanoTime() - tp_core_start[thread_id];
                if (size == 0)
                    size = 1;
                tp_core_event[thread_id] = tp_core[thread_id] / size;
            }
        }

        public static void BEGIN_TP_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp[thread_id] = System.nanoTime() - tp_start[thread_id];
        }

        public static void END_TRANSACTION_TIME_MEASURE(int thread_id) {

            if (CONTROL.enable_profile && measure_counts[thread_id]++ < CONTROL.MeasureBound) {
                //                LOG.info("wait time:" + (txn_wait[thread_id]));
                txn_total[thread_id] = (System.nanoTime() - txn_start[thread_id]) / 1E6;
                //                        (prepare_time[thread_id] //includes event extraction time.
                //                                + txn_wait[thread_id]
                //                                + index_time[thread_id]
                //                                + tp_core[thread_id]
                //                                + abort_time[thread_id]
                //                                + compute_total[thread_id]//includes compute and emit.
                //                                + System.nanoTime() - compute_end[thread_id]//commit time.
                //                        ) / 1E6;

                metrics.exe_time[thread_id].addValue(compute_total[thread_id]);

                metrics.useful_time[thread_id].addValue((prepare_time[thread_id] + compute_total[thread_id] + tp_core[thread_id]) / txn_total[thread_id]);

                metrics.index_time[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);


                metrics.lock[thread_id].addValue((txn_lock[thread_id]) / txn_total[thread_id]);


                metrics.wait[thread_id].addValue((txn_wait[thread_id]) / txn_total[thread_id]);

                metrics.abort_time[thread_id].addValue(abort_time[thread_id] / txn_total[thread_id]);

                metrics.ts_allocation[thread_id].addValue(ts_allocate[thread_id] / txn_total[thread_id]);

                index_time[thread_id] = 0;
            }
        }

        //needs to include requests construction time.
        public static void END_TRANSACTION_TIME_MEASURE_TS(int thread_id) {

            if (CONTROL.enable_profile && measure_counts[thread_id]++ < CONTROL.MeasureBound) {
                txn_total[thread_id] = (System.nanoTime() - txn_start[thread_id] + read_handle[thread_id] + write_handle[thread_id]) / 1E6;
                //                        (prepare_time[thread_id]
                //                                + read_handle[thread_id]
                //                                + write_handle[thread_id]
                //                                + tp[thread_id]//tp_core+wait+index
                //                                + compute_total[thread_id]
                //                                // + abort_time[thread_id]
                //                                // (System.nanoTime() - compute_start[thread_id]//commit time. T-Stream does not have commit time.
                //                        ) / 1E6;

                metrics.useful_time[thread_id].addValue((prepare_time[thread_id] + compute_total[thread_id] + tp_core[thread_id] - tp_submit[thread_id]) / txn_total[thread_id]);

                metrics.exe_time[thread_id].addValue(compute_total[thread_id]);

                metrics.index_time[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);

                metrics.lock[thread_id].addValue((txn_lock[thread_id]) / txn_total[thread_id]);

                metrics.wait[thread_id].addValue((tp[thread_id] - tp_core[thread_id] + tp_submit[thread_id]) / txn_total[thread_id]);

                //                metrics.abort_time[thread_id].addValue(abort_time[thread_id] / txn_total[thread_id]);

                metrics.average_tp_event[thread_id].addValue(tp_core_event[thread_id]);

                metrics.average_tp[thread_id].addValue(tp_core[thread_id]);

                metrics.average_tp_submit[thread_id].addValue(tp_submit[thread_id]);

                metrics.average_tp_w_syn[thread_id].addValue(tp[thread_id]);
                //clean;
                index_time[thread_id] = 0;
                read_handle[thread_id] = 0;
                write_handle[thread_id] = 0;
                prepare_time[thread_id] = 0;
            }
        }

    }
}
