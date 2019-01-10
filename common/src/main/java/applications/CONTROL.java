package applications;

public interface CONTROL {

    //application related.
    int NUM_EVENTS = 500_000; //500_000 : 1_000_000; //1_000_000 for real use cases;

    //db related.
    boolean enable_shared_state = true;

    boolean enable_states_partition = false;//only enable for PAT.

    //latency related.
    boolean enable_admission_control = true;//only enable for TStream
    boolean enable_latency_measurement = false;//

    //profile related.
    boolean enable_profile = false;//enable this only when we want to test for breakdown.
    boolean enable_debug = false;//some critical debug section.


    //engine related.
    boolean enable_engine = true;//1. enable TP_engine. Always enabled. There's no meaning if we disable engine for T-Stream.
    boolean enable_work_stealing = true; // 2. this is a sub-option, only useful when engine is enabled.
    boolean enable_mvcc = enable_work_stealing;// always enabled in CT and enable if work_stealing is enabled.
    boolean enable_numa_placement = false;//3. numa placement.
    boolean enable_speculative = false;//work in future!

    //used for fixed-partition engine (no work-stealing).
    int island = -1;//-1 stands for one engine per core; if NUMA-unaware this is set to #sockets by default (that is spread by default).
    int CORE_PER_SOCKET = 10;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 4;//configure this for NUMA placement please.

    //global settings.
    int kMaxThreadNum = 40;
    int MeasureBound = 100_000;


}