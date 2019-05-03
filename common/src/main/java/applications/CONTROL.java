package applications;

public interface CONTROL {

    //global settings.
    int kMaxThreadNum = 40;
    int MeasureBound = 100_000;

    //application related.
    int NUM_EVENTS = 1_000_000; //for fast test.

    //combo optimization
    boolean enable_app_combo = true;//compose all operators into one.

    int combo_bid_size = 500;//reduce conflict. NOT applicable to LAL, LWM and PAT (must set to one).

    int MIN_EVENTS_PER_THREAD = NUM_EVENTS / combo_bid_size / kMaxThreadNum;


    //order related.

    boolean enable_force_ordering = true;

    //db related.
    boolean enable_shared_state = true;//this is for transactional state mgmt.

    boolean enable_states_partition = true;//must be enabled for PAT/SSTORE.

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

    boolean enable_work_stealing = true; // 2. this is a sub-option, only useful when engine is enabled. Still BUGGY, resolve in future.
    boolean enable_mvcc = enable_work_stealing;// always enabled in CT and enable if work_stealing is enabled.
    boolean enable_speculative = false;//work in future!

    //used for fixed-partition engine (no work-stealing).
    int island = -1;//-1 stands for one engine per core; if NUMA-unaware this is set to #sockets by default (that is spread by default).
    int CORE_PER_SOCKET = 10;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 4;//configure this for NUMA placement please.


}