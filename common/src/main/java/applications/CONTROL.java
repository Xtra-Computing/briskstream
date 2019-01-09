package applications;

public interface CONTROL {

    //application related.
    int num_events = 1_000_000; //20_000_000;

    //db related.
    boolean enable_shared_state = true;

    //latency related.
    boolean enable_admission_control = false;//only enable when we want to test latency?
    boolean enable_latency_measurement = false;//

    //profile related.
    boolean enable_profile = false;//enable this only when we want to test for breakdown.
    boolean enable_debug = false;//some critical debug section.


    //engine related.
    boolean enable_mvcc = false; // always enabled in CT and disabled in others.
    boolean enable_engine = true;//1. enable TP_engine. Always enabled. There's no meaning if we disable engine for T-Stream.
    boolean enable_work_stealing = true;//2. this is a sub-option, only useful when engine is enabled.
    boolean enable_numa_placement = false;//3. numa placement.
    boolean enable_speculative = false;//work in future!

    //used for fixed-partition engine (no work-stealing).
    int island = -1;//-1 stands for one engine per core; if NUMA-unaware this is set to #sockets by default (that is spread by default).
    int CORE_PER_SOCKET = 10;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 4;//configure this for NUMA placement please.

    //global settings.
    int kMaxThreadNum = 40;
    int MeasureBound = 100_000;
    int SIZE_EVENTS = 50_000;


}