package applications.spout;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.TopologyComponent;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import static applications.constants.PositionKeepingConstants.Constant.NUM_MACHINES;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_EVENT;
import static engine.content.Content.CCOption_SStore;
import static engine.content.Content.CCOption_TStream;
import static utils.PartitionHelper.key_to_partition;

public class PKSpout extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBenchmarkSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;

    /**
     * key: device ID,
     * Value: current value_list.
     *
     * @throws InterruptedException
     */


    private transient FastZipfGenerator keygenerator;
    private long[] p_bid;


    public PKSpout() {
        super(LOG);
        this.scalable = false;
        state = new ValueState();
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    private Set[] input_keys = new Set[10_000];

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        long start = System.nanoTime();

        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..

        String OS_prefix = null;


        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);

        keygenerator = new FastZipfGenerator(NUM_MACHINES, theta, 0);

        ccOption = config.getInt("CCOption", 0);
        bid = 0;

        tthread = config.getInt("tthread");

        p_bid = new long[tthread];

        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }

        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);

        boolean[] multi_partion_decision;
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

        int number_partitions = config.getInt("number_partitions");

        for (TopologyComponent children : this.context.getThisComponent().getChildrenOfStream().keySet()) {
            int numTasks = children.getNumTasks();
            total_children_tasks += numTasks;
        }

        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);


        for (int k = 0; k < 10_000; k++) {
            Set<Integer> keys = new LinkedHashSet<>();
            for (int i = 0; i < SIZE_EVENT; i++) {
                int key = keygenerator.next();
                while (keys.contains(key)) {
                    key = keygenerator.next();
                }
                keys.add(key);
            }
            input_keys[k] = keys;
        }


        long end = System.nanoTime();
        LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);

    }


    private void spout_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("spout_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.relax_reset();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    Random r = new Random();

    /**
     * There is no idea which key shall the event touch.
     * Each event contains 10 different keys.
     *
     * @throws InterruptedException
     */
    @Override
    public void nextTuple() throws InterruptedException {


//        LOG.debug(Arrays.toString(p_bid));
        Set<Integer> keys = new LinkedHashSet<>();
        keys.addAll(input_keys[counter++ % 10_000]);

        if (ccOption == CCOption_SStore) {
            collector.emit_single(p_bid.clone(), bid, keys);//combined R/W executor.
            for (Integer key : keys) {
                int p = key_to_partition(key);//the partition to touch.
                p_bid[p]++;
            }

        } else {
            if (ccOption == CCOption_TStream) {
                if (control < target_Hz) {
                    collector.emit_single(bid, keys);//combined R/W executor.
                    control++;
                } else
                    empty++;

                forward_checkpoint(-1, bid, null); // This is required by T-Stream.
            } else {
                collector.emit_single(bid, keys);//combined R/W executor.
            }
        }
        bid++;
    }

    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {

    }

}