package applications.topology.transactional.initializer;

import applications.topology.transactional.State;
import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import engine.Database;
import engine.common.SpinLock;

import java.util.Set;

public abstract class TableInitilizer {
    protected final Database db;
    protected final double scale_factor;
    protected final double theta;
    protected final int tthread;
    int floor_interval;

    public TableInitilizer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        this.db = db;
        this.scale_factor = scale_factor;
        this.theta = theta;
        this.tthread = tthread;
        State.initilize(config);
    }

    public void loadData(int maxContestants, String contestants) {
        throw new UnsupportedOperationException();
    }

    public abstract void creates_Table();


    public abstract void loadData(int thread_id, TopologyContext context);

    public abstract void loadData(int thread_id, SpinLock[] spinlock, TopologyContext context);

    public abstract void loadData(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_);

    public abstract void loadData(double scale_factor, double theta);

    protected int get_pid(int partition_interval, int key) {
        return (int) Math.floor(key / (double) partition_interval);//NUM_ITEMS / tthread;

    }


    boolean verify(Set keys, int partition_id, int number_of_partitions) {

        for (Object key : keys) {
            int i = (Integer) key;
            int pid = i / (floor_interval);

            boolean case1 = pid >= partition_id && pid <= partition_id + number_of_partitions;
            boolean case2 = pid >= 0 && pid <= (partition_id + number_of_partitions) % tthread;

            if (!(case1 || case2)) {
                return false;
            }
        }

        return true;
    }


}
