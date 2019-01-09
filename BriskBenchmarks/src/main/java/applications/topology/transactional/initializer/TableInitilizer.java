package applications.topology.transactional.initializer;

import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import applications.param.ob.OBParam;
import applications.tools.FastZipfGenerator;
import applications.topology.transactional.State;
import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import engine.Database;
import engine.benchmark.TxnParam;
import engine.common.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.SplittableRandom;

import static applications.CONTROL.NUM_EVENTS;
import static applications.topology.transactional.State.partioned_store;
import static engine.profiler.Metrics.NUM_ITEMS;
import static utils.PartitionHelper.key_to_partition;

public abstract class TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TableInitilizer.class);
    protected final Database db;
    protected final double scale_factor;
    protected final double theta;
    protected final int tthread;
    int floor_interval;

    protected long[] p_bid;//used for partition.
    protected transient FastZipfGenerator p_generator;
    protected int number_partitions;
    protected boolean[] multi_partion_decision;
    SplittableRandom rnd = new SplittableRandom(1234);

    int j = 0;
    int p;

    protected final String split_exp = ";";

    //dual-decision
    protected transient int[] dual_decision = new int[]{0, 0, 0, 0, 1, 1, 1, 1};//1:1 deposite and transfer;

    private int i = 0;
    protected int next_decision2() {

        int rt = dual_decision[i];
        i++;
        if (i == 8)
            i = 0;
        return rt;
    }

    public TableInitilizer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        this.db = db;
        this.scale_factor = scale_factor;
        this.theta = theta;
        this.tthread = tthread;
        State.initilize(config);
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);

        if (ratio_of_multi_partition == 0) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, false};// all single.
        } else if (ratio_of_multi_partition == 0.125) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, false, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.25) {
            multi_partion_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% single, 25% multi.
        } else if (ratio_of_multi_partition == 0.5) {
            multi_partion_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal ratio.
        } else if (ratio_of_multi_partition == 0.75) {
            multi_partion_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 0.875) {
            multi_partion_decision = new boolean[]{false, true, true, true, true, true, true, true};//25% single, 75% multi.
        } else if (ratio_of_multi_partition == 1) {
            multi_partion_decision = new boolean[]{true, true, true, true, true, true, true, true};// all multi.
        } else {
            throw new UnsupportedOperationException();
        }

        LOG.info("ratio_of_multi_partition: " + ratio_of_multi_partition + "\tDECISIONS: " + Arrays.toString(multi_partion_decision));

        p_bid = new long[tthread];

        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }
        floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);
        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
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

    protected void randomkeys(int pid, TxnParam param, Set keys, int access_per_partition, int counter, int numAccessesPerBuy) {
        for (int access_id = 0; access_id < numAccessesPerBuy; ++access_id) {
            FastZipfGenerator generator = partioned_store[pid];
            int res = generator.next();
            //should not have duplicate keys.
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS) {
//                    res = partition_id * interval;
//                }
                res = generator.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
            counter++;
            if (counter == access_per_partition) {
//                pointer++;
                pid++;
                if (pid == tthread)
                    pid = 0;

                counter = 0;
            }
        }
    }

    protected abstract boolean load(String file) throws IOException;

    public void prepare_input_events(String file_path) throws IOException {

        db.eventManager.ini(NUM_EVENTS);

        //try to read from file.
        if (!load(file_path + tthread)) {
            //if failed, create new one.
            Object event;
            for (int i = 0; i < NUM_EVENTS; i++) {
                boolean multi_parition_txn_flag = multi_partion_decision[j];
                j++;
                if (j == 8)
                    j = 0;

                if (multi_parition_txn_flag) {//multi-partition
                    p = key_to_partition(p_generator.next());//randomly pick a starting point.
                    event = create_new_event(number_partitions, i);

                    if (event instanceof DepositEvent)
                        number_partitions = 2;
                    else if (event instanceof TransactionEvent)
                        number_partitions = 4;

                    for (int k = 0; k < number_partitions; k++) {//depo event only allows 2 partition
                        p_bid[p]++;
                        p++;
                        if (p == tthread)
                            p = 0;
                    }

                } else {
                    event = create_new_event(1, i);
                    p_bid[p]++;
                    p++;
                    if (p == tthread)
                        p = 0;
                }
                db.eventManager.put(event, i);
            }
            dump(file_path + tthread);
        }
    }

    protected abstract void dump(String file_path) throws IOException;

    protected abstract Object create_new_event(int number_partitions, int bid);
}
