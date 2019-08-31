package applications;

public interface CONTROL {

    //application related.
    int NUM_EVENTS = 1_000_000; //500_000 : 1_000_000; //1_000_000 for real use cases;


    //latency related.
    boolean enable_latency_measurement = true;//

    //profile related.
    boolean enable_profile = false;//enable this only when we want to test for breakdown.
    boolean enable_debug = false;//some critical debug section.


}