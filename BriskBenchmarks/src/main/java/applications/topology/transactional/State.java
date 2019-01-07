package applications.topology.transactional;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;

import static applications.constants.CrossTableConstants.Constant.NUM_ACCOUNTS;
import static engine.profiler.Metrics.NUM_ITEMS;


public class State {
    public static FastZipfGenerator shared_store;
    public static FastZipfGenerator[] partioned_store;

    public static void initilize(Configuration config) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);
        int tthread = config.getInt("tthread", 0);
        int floor_interval;
        switch (config.getString("application")) {

            case "OnlineBiding": {
                floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
                partioned_store = new FastZipfGenerator[tthread];//total number of working threads.
                for (int i = 0; i < tthread; i++) {
                    partioned_store[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval);
                }
                break;
            }
            case "CrossTables": {
                if (config.getBoolean("partition")) {//only used in sstore.
//                    interval = (int) Math.ceil(NUM_ACCOUNTS / (double) tthread);//NUM_ITEMS / tthread;
                    floor_interval = (int) Math.floor(NUM_ACCOUNTS / (double) tthread);//NUM_ITEMS / tthread;
                    partioned_store = new FastZipfGenerator[tthread];//total number of working threads.
                    for (int i = 0; i < tthread; i++) {
                        partioned_store[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval);
                    }

                } else {
                    shared_store = new FastZipfGenerator((int) (NUM_ACCOUNTS * scale_factor), theta, 0);
                }
                break;
            }
            case "MicroBenchmark": {
                if (config.getBoolean("partition")) {//only used in sstore.

//                    interval = (int) Math.ceil(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
                    floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
                    partioned_store = new FastZipfGenerator[tthread];//total number of working threads.
                    for (int i = 0; i < tthread; i++) {
                        partioned_store[i] = new FastZipfGenerator((int) (floor_interval * scale_factor), theta, i * floor_interval);
                    }

                } else {
                    shared_store = new FastZipfGenerator((int) (NUM_ITEMS * scale_factor), theta, 0);
                }
                break;
            }

        }
    }
}
