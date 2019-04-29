package applications;

public interface CONTROL {

    //application related.
    int NUM_EVENTS = 100_000; //500_000 : 1_000_000; //5_000_000 for real use cases;

    //order related.

    boolean enable_force_ordering = true;

    //db related.
    boolean enable_shared_state = true;//this is for transactional state mgmt.

    boolean enable_states_partition = false;

    boolean enable_TSTREAM = false;


    //latency related.

    boolean enable_latency_measurement = false;//
    boolean enable_admission_control = enable_latency_measurement;//only enable for TStream

    //profile related.
    boolean enable_profile = true;//enable this only when we want to test for breakdown.
    boolean enable_debug = false;//some critical debug section.

    //engine related.
    boolean enable_engine = true;//1. enable TP_engine. Always enabled. There's no meaning if we disable engine for T-Stream.
    boolean enable_numa_placement = true;//3. numa placement.

    boolean enable_work_stealing = false; // 2. this is a sub-option, only useful when engine is enabled. Still BUGGY, resolve in future.
    boolean enable_mvcc = enable_work_stealing;// always enabled in CT and enable if work_stealing is enabled.
    boolean enable_speculative = false;//work in future!

    //used for fixed-partition engine (no work-stealing).
    int island = -1;//-1 stands for one engine per core; if NUMA-unaware this is set to #sockets by default (that is spread by default).
    int CORE_PER_SOCKET = 10;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 4;//configure this for NUMA placement please.

    //global settings.
    int kMaxThreadNum = 40;
    int MeasureBound = 100_000;
}