package engine.profiler;

import applications.CONTROL;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import static engine.Meta.MetaTypes.kMaxThreadNum;

public class Metrics {


    private static Metrics ourInstance = new Metrics();

    public static int COMPUTE_COMPLEXITY = 10;//default setting. 1, 10, 100
    public static int POST_COMPUTE_COMPLEXITY = 1;
    public static int NUM_ACCESSES = 10;//10 as default setting. 2 for short transaction, 10 for long transaction.? --> this is the setting used in YingJun's work. 16 is the default value_list used in 1000core machine.
    public static int NUM_ITEMS = 1_000_000;//1. 1_000_000; 2. ? ; 3. 1_000  //1_000_000 YCSB has 16 million records, Ledger use 200 million records.
    public static int H2_SIZE;

    public DescriptiveStatistics[] txn_total = new DescriptiveStatistics[kMaxThreadNum];//overhead_total time spend in txn.
    public DescriptiveStatistics[] stream_total = new DescriptiveStatistics[kMaxThreadNum];//overhead_total time spend in txn.
    public DescriptiveStatistics[] overhead_total = new DescriptiveStatistics[kMaxThreadNum];//overhead_total time spend in txn.


//    public DescriptiveStatistics[] exe_ratio = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.

//    public volatile boolean measure = false;

    //    public void start_measurement() {
//        measure = true;
//    }
    public DescriptiveStatistics[] useful_ratio = new DescriptiveStatistics[kMaxThreadNum];//useful_work time.
    //    public DescriptiveStatistics[] average_tp_event = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per event thread without considering synchronization.
    public DescriptiveStatistics[] abort_ratio = new DescriptiveStatistics[kMaxThreadNum];//abort


    // Op id, descriptive
//    public Map<String, DescriptiveStatistics> useful_ratio = new HashMap<>();//useful_work time.
//    public Map<String, DescriptiveStatistics> abort_ratio = new HashMap<>();//abort
//    public Map<String, DescriptiveStatistics> ts_allocation = new HashMap<>();//timestamp allocation
//    public Map<String, DescriptiveStatistics> index_ratio = new HashMap<>();//index
//    public Map<String, DescriptiveStatistics> sync_ratio = new HashMap<>();// sync_ratio lock_ratio and order.
//    public Map<String, DescriptiveStatistics> exe_ratio = new HashMap<>();//not in use.
//    public Map<String, DescriptiveStatistics> order_wait = new HashMap<>();//order sync_ratio
//    public Map<String, DescriptiveStatistics> enqueue_time = new HashMap<>();//event enqueue

    //TODO: single op for now. per task/thread.
//    public DescriptiveStatistics[] ts_allocation = new DescriptiveStatistics[kMaxThreadNum];//timestamp allocation
    public DescriptiveStatistics[] index_ratio = new DescriptiveStatistics[kMaxThreadNum];//index
    public DescriptiveStatistics[] sync_ratio = new DescriptiveStatistics[kMaxThreadNum];// sync_ratio lock_ratio and order.
    public DescriptiveStatistics[] lock_ratio = new DescriptiveStatistics[kMaxThreadNum];// sync_ratio lock_ratio and order.
    public DescriptiveStatistics[] average_tp_core = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread without considering synchronization.
    public DescriptiveStatistics[] average_tp_submit = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread without considering synchronization.
    public DescriptiveStatistics[] average_tp_w_syn = new DescriptiveStatistics[kMaxThreadNum];// average tp processing time per thread with synchronization.
    public DescriptiveStatistics[] average_txn_construct = new DescriptiveStatistics[kMaxThreadNum];
//    public Map<Integer, DescriptiveStatistics> exe_ratio = new HashMap<>();//not in use.
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
//        exe_ratio.put(ID, new DescriptiveStatistics());
//        useful_ratio.put(ID, new DescriptiveStatistics());
//        abort_ratio.put(ID, new DescriptiveStatistics());
//        index_ratio.put(ID, new DescriptiveStatistics());
//        sync_ratio.put(ID, new DescriptiveStatistics());
//        ts_allocation.put(ID, new DescriptiveStatistics());
//        enqueue_time.put(ID, new DescriptiveStatistics());

        NUM_ACCESSES = num_access;
    }

    public void initilize(int task) {

        txn_total[task] = new DescriptiveStatistics();

        stream_total[task] = new DescriptiveStatistics();

        overhead_total[task] = new DescriptiveStatistics();

        useful_ratio[task] = new DescriptiveStatistics();
//        exe_ratio[task] = new DescriptiveStatistics();
        abort_ratio[task] = new DescriptiveStatistics();
//        ts_allocation[task] = new DescriptiveStatistics();
        index_ratio[task] = new DescriptiveStatistics();
        sync_ratio[task] = new DescriptiveStatistics();
        lock_ratio[task] = new DescriptiveStatistics();
        average_tp_core[task] = new DescriptiveStatistics();
        average_txn_construct[task] = new DescriptiveStatistics();
        average_tp_submit[task] = new DescriptiveStatistics();
        average_tp_w_syn[task] = new DescriptiveStatistics();
//        enqueue_time[task] = new DescriptiveStatistics();
    }


    public static class MeasureTools {
        protected static Metrics metrics = getInstance();


        static long[] stream_start = new long[kMaxThreadNum];

        static long[] txn_start = new long[kMaxThreadNum];

        static double[] txn_total = new double[kMaxThreadNum];
        static long[] txn_wait_start = new long[kMaxThreadNum];
        static long[] txn_wait = new long[kMaxThreadNum];
        static long[] txn_lock_start = new long[kMaxThreadNum];
        static long[] txn_lock = new long[kMaxThreadNum];
        static long[] prepare_start = new long[kMaxThreadNum];
        static long[] prepare_time = new long[kMaxThreadNum];
        static long[] post_time_start = new long[kMaxThreadNum];
        static long[] post_time = new long[kMaxThreadNum];
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
        static long[] pre_txn_start = new long[kMaxThreadNum];
        static long[] pre_txn_total = new long[kMaxThreadNum];
        static long[] write_handle_start = new long[kMaxThreadNum];
        static long[] write_handle = new long[kMaxThreadNum];

        static long[] tp_start = new long[kMaxThreadNum];
        static long[] tp = new long[kMaxThreadNum];//tp=tp_core + tp_submit
        static long[] tp_core_start = new long[kMaxThreadNum];
        static double[] tp_core = new double[kMaxThreadNum];
        static long[] tp_submit_start = new long[kMaxThreadNum];
        static double[] tp_submit = new double[kMaxThreadNum];

        static long[] sync = new long[kMaxThreadNum];//further sync

        static long[] tp_core_event = new long[kMaxThreadNum];

        public static long[] measure_counts = new long[kMaxThreadNum];

        public static void BEGIN_TRANSACTION_TIME_MEASURE(int thread_id) {

            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_start[thread_id] = System.nanoTime();
        }

        public static void BEGIN_PREPARE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                prepare_start[thread_id] = System.nanoTime();
                if (stream_start[thread_id] == 0) {//first time measurement.
                    stream_start[thread_id] = prepare_start[thread_id];
                }
            }
        }

        public static void END_PREPARE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                prepare_time[thread_id] = System.nanoTime() - prepare_start[thread_id];
            }
        }


        public static void BEGIN_POST_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                post_time_start[thread_id] = System.nanoTime();
        }

        public static void END_POST_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                post_time[thread_id] = System.nanoTime() - post_time_start[thread_id];
            }
        }

        public static void END_POST_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                post_time[thread_id] += System.nanoTime() - post_time_start[thread_id];
            }
        }

        public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_lock_start[thread_id] = System.nanoTime();
        }

        public static long END_LOCK_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                long rt = System.nanoTime() - txn_lock_start[thread_id];
                txn_lock[thread_id] += rt;
                return rt;
            }
            return 0;
        }


        public static void END_LOCK_TIME_MEASURE_NOCC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                long rt = System.nanoTime() - txn_lock_start[thread_id];
                txn_lock[thread_id] += rt - tp_core_event[thread_id] - index_time[thread_id];
            }
        }

        public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_wait_start[thread_id] = System.nanoTime();
        }

        public static void END_WAIT_TIME_MEASURE_ACC(int thread_id, long lock_time_measure) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                txn_wait[thread_id] += (System.nanoTime() - txn_wait_start[thread_id] - lock_time_measure);
        }

        public static void BEGIN_COMPUTE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                compute_start[thread_id] = System.nanoTime();
        }

        public static void END_COMPUTE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                compute_total[thread_id] = System.nanoTime() - compute_start[thread_id];
            }
        }

        public static void END_COMPUTE_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                compute_total[thread_id] += System.nanoTime() - compute_start[thread_id];
            }
        }

        //needs to include write compute time also for TS.
        public static void END_COMPUTE_TIME_MEASURE_TS(int thread_id, double write_useful_time, int read_size, int write_size) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                if (read_size == 0) {
                    compute_total[thread_id] = (write_useful_time * write_size);
                } else {
                    compute_total[thread_id] = (double) (System.nanoTime() - compute_start[thread_id]) + (write_useful_time * write_size);

                }
            }
        }

        public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                index_start[thread_id] = System.nanoTime();
        }

        public static void END_INDEX_TIME_MEASURE(int thread_id, boolean is_retry_) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                if (!is_retry_)
                    index_time[thread_id] = System.nanoTime() - index_start[thread_id];
            }
        }

        public static void END_INDEX_TIME_MEASURE_ACC(int thread_id, boolean is_retry_) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                if (!is_retry_)
                    index_time[thread_id] += System.nanoTime() - index_start[thread_id];
            }
        }

        public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_start[thread_id] = System.nanoTime();
        }

        public static void END_ABORT_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_time[thread_id] += System.nanoTime() - abort_start[thread_id];
        }

        public static void CLEAN_ABORT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                abort_time[thread_id] = 0;
        }

        public static void BEGIN_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                ts_allocate_start[thread_id] = System.nanoTime();
        }

        public static void END_TS_ALLOCATE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                ts_allocate[thread_id] = System.nanoTime() - ts_allocate_start[thread_id];
        }

        //t-stream special

        public static void BEGIN_PRE_TXN_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                pre_txn_start[thread_id] = System.nanoTime();
        }

        public static long END_PRE_TXN_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                long rt = System.nanoTime() - pre_txn_start[thread_id];
                pre_txn_total[thread_id] += rt;
                return rt;
            }
            return 0;
        }

        public static void BEGIN_WRITE_HANDLE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                write_handle_start[thread_id] = System.nanoTime();
        }

        public static void END_WRITE_HANDLE_TIME_MEASURE_TS(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                write_handle[thread_id] += System.nanoTime() - write_handle_start[thread_id];
        }


        public static void BEGIN_TP_SUBMIT_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_submit_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_SUBMIT_TIME_MEASURE(int thread_id, int size) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                tp_submit[thread_id] = (double) (System.nanoTime() - tp_submit_start[thread_id]);
            }
        }

        public static void BEGIN_TP_CORE_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_core_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_CORE_TIME_MEASURE_ACC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                tp_core[thread_id] += System.nanoTime() - tp_core_start[thread_id];
            }
        }

        public static void END_TP_CORE_TIME_MEASURE_NOCC(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                long duration = System.nanoTime() - tp_core_start[thread_id];
                tp_core[thread_id] += duration;
//                tp_core_event[thread_id] = duration;
            }
        }

        public static void END_TP_CORE_TIME_MEASURE_TS(int thread_id, int size) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                tp_core[thread_id] = (System.nanoTime() - tp_core_start[thread_id] - tp_submit[thread_id]);
            }
        }

        public static void BEGIN_TP_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp_start[thread_id] = System.nanoTime();
        }

        public static void END_TP_TIME_MEASURE(int thread_id) {
            if (CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound)
                tp[thread_id] = System.nanoTime() - tp_start[thread_id];
        }

        public static void END_TRANSACTION_TIME_MEASURE(int thread_id) {

            if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {

                txn_total[thread_id] = (System.nanoTime() - txn_start[thread_id]);

                metrics.useful_ratio[thread_id].addValue((tp_core[thread_id] + compute_total[thread_id]) / txn_total[thread_id]);

                metrics.index_ratio[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);

                metrics.lock_ratio[thread_id].addValue((txn_lock[thread_id]) / txn_total[thread_id]);

                metrics.sync_ratio[thread_id].addValue((txn_wait[thread_id]) / txn_total[thread_id]);

                metrics.abort_ratio[thread_id].addValue(abort_time[thread_id] / txn_total[thread_id]);


            }
        }

        //compute per event time spent.
        public static void END_TOTAL_TIME_MEASURE_ACC(int thread_id, int combo_bid_size) {


            if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {

                long current_time = System.nanoTime();
                long overall_processing_time_per_batch = current_time - stream_start[thread_id];
                stream_start[thread_id] = current_time;

                long stream_processing = (prepare_time[thread_id] + post_time[thread_id]) / combo_bid_size;
                metrics.stream_total[thread_id].addValue(stream_processing);
                metrics.overhead_total[thread_id].addValue((overall_processing_time_per_batch - txn_total[thread_id] - stream_processing) / combo_bid_size);

//                metrics.overhead_total[thread_id].addValue((double) (prepare_time[thread_id] + post_time[thread_id]) / combo_bid_size);
                metrics.txn_total[thread_id].addValue(txn_total[thread_id] / combo_bid_size);

                //clean.
                compute_total[thread_id] = 0;
                tp_core[thread_id] = 0;
                index_time[thread_id] = 0;
                txn_lock[thread_id] = 0;
                txn_wait[thread_id] = 0;
                abort_time[thread_id] = 0;
                prepare_time[thread_id] = 0;
                post_time[thread_id] = 0;
            }
            measure_counts[thread_id]++;
        }


        static long end_time;

        //needs to include requests construction time /* pre_txn_total */.
        // ! Thread.interrupted() will clear the interrupt flag!! be very careful!!
        public static void END_TRANSACTION_TIME_MEASURE_TS(int thread_id) {
            if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {
                end_time = System.nanoTime();

                pre_txn_total[thread_id] -= post_time[thread_id];//need to exclude write-post time.

                txn_total[thread_id] = ((end_time - txn_start[thread_id] + pre_txn_total[thread_id]));

                metrics.useful_ratio[thread_id].addValue((compute_total[thread_id] + tp_core[thread_id]) / txn_total[thread_id]);

                metrics.index_ratio[thread_id].addValue(index_time[thread_id] / txn_total[thread_id]);

                metrics.lock_ratio[thread_id].addValue(0);

                metrics.abort_ratio[thread_id].addValue(0);

                metrics.sync_ratio[thread_id].addValue((tp[thread_id] - tp_core[thread_id]) / txn_total[thread_id]);


            }
        }

        //compute per event time spent.
        public static void END_TOTAL_TIME_MEASURE_TS(int thread_id, int txn_size) {

            if (!Thread.currentThread().isInterrupted() && CONTROL.enable_profile && CONTROL.MeasureStart < measure_counts[thread_id] && measure_counts[thread_id] < CONTROL.MeasureBound) {

                long current_time = System.nanoTime();

                long overall_processing_time_per_wm = current_time - stream_start[thread_id];//time from receiving first event to finish last event in the current batch.

                long stream_processing = (prepare_time[thread_id] + post_time[thread_id]) / txn_size;
                metrics.stream_total[thread_id].addValue(stream_processing);
                metrics.overhead_total[thread_id].addValue((overall_processing_time_per_wm - txn_total[thread_id] - stream_processing) / txn_size);

                metrics.txn_total[thread_id].addValue(txn_total[thread_id] / txn_size);
                metrics.average_tp_core[thread_id].addValue(tp_core[thread_id] / txn_size);
                metrics.average_tp_submit[thread_id].addValue(tp_submit[thread_id] / txn_size);
                metrics.average_txn_construct[thread_id].addValue((double) pre_txn_total[thread_id] / txn_size);
                metrics.average_tp_w_syn[thread_id].addValue((double) tp[thread_id] / txn_size);

                //clean;
                stream_start[thread_id] = current_time;
                compute_total[thread_id] = 0;
                index_time[thread_id] = 0;
                pre_txn_total[thread_id] = 0;
                write_handle[thread_id] = 0;
                prepare_time[thread_id] = 0;
                tp_core[thread_id] = 0;
                tp_submit[thread_id] = 0;
                tp[thread_id] = 0;
                post_time[thread_id] = 0;
            }
            measure_counts[thread_id] += txn_size;
        }
    }
}
