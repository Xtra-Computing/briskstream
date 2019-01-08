package applications;

public interface CONTROL {

    //application related.
    int num_events = 1_000_000; //20_000_000;

    //db related.
    boolean enable_shared_state = true;

    //latency related.
    boolean enable_admission_control = false;//only enable when we want to test latency?
    boolean enable_latency_measurement = true;//

    //profile related.
    boolean enable_profile = false;//enable this only when we want to test for breakdown.
    boolean enable_debug = false;//some critical debug section.


    //engine related.
    boolean enable_mvcc = false; // always enabled in CT and disabled in others.
    boolean enable_numa_placement = false;//1. numa placement.
    boolean enable_engine = false;//2. enable TP_engine.
    boolean enable_work_stealing = false;//3. this is a sub-option, only useful when engine is enabled.
    boolean enable_speculative = false;//work in future!

    //used for multi-engine.
    boolean enable_multi_engine = false;//harmful. study in future.
    int island = 2;//if NUMA-unaware this is set to #sockets by default (that is spread by default).
    int CORE_PER_SOCKET = 10;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 4;//configure this for NUMA placement please.
    int TOTAL_CORES = 40;

    //global settings.
    int kMaxThreadNum = 40;
    int MeasureBound = 100_000;
    int SIZE_EVENTS = 50_000;


}