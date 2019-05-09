package applications.spout;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
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
import java.util.Random;

import static applications.CONTROL.enable_admission_control;
import static applications.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static engine.content.Content.CCOption_SStore;
import static engine.content.Content.CCOption_TStream;
import static utils.PartitionHelper.key_to_partition;

public class CrossTablesSpout extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBenchmarkSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;

    int i = 0;
    int cnt = 0;
    int j = 0;

    private Random r = new Random();
    //different R-W ratio.
    //just enable one of the decision array
    private boolean[] read_decision;
    protected long[] p_bid;
    protected int number_partitions;


    public CrossTablesSpout() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        long start = System.nanoTime();

        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..

        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("JVM PID  = " + pid);

        long end = System.nanoTime();
        LOG.info("spout initialize takes (ms):" + (end - start) / 1E6);
        ccOption = config.getInt("CCOption", 0);
        bid = 0;

        tthread = config.getInt("tthread");

        p_bid = new long[tthread];

        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }

        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

        if (ratio_of_read == 0) {
            read_decision = new boolean[]{false, false, false, false, false, false, false, false};// all write.
        } else if (ratio_of_read == 0.25) {
            read_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% W, 25% R.
        } else if (ratio_of_read == 0.5) {
            read_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal r-w ratio.
        } else if (ratio_of_read == 0.75) {
            read_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% W, 75% R.
        } else if (ratio_of_read == 1) {
            read_decision = new boolean[]{true, true, true, true, true, true, true, true};// all read.
        } else {
            throw new UnsupportedOperationException();
        }

        LOG.info("ratio_of_read: " + ratio_of_read + "\tREAD DECISIONS: " + Arrays.toString(read_decision));

        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);

        if (tthread == 2) {
            ratio_of_multi_partition = 0;
        } else ratio_of_multi_partition = 1;

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

        number_partitions = 4;//config.getInt("number_partitions");

        for (TopologyComponent children : this.context.getThisComponent().getChildrenOfStream().keySet()) {
            int numTasks = children.getNumTasks();
            total_children_tasks += numTasks;
        }

        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);
        p_generator = new FastZipfGenerator(NUM_ACCOUNTS, theta, 0);

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

    @Override
    public void nextTuple() throws InterruptedException {
        if (ccOption == CCOption_SStore) {

//            boolean flag = read_decision[i];
            //This ensures a balanced workload among executors.
//            cnt++;
//            if (cnt % total_children_tasks == 0) {
//                i++;
//                if (i == 8)
//                    i = 0;
//            }

            int p = key_to_partition(p_generator.next());//randomly pick a starting point.

            collector.emit_single(p_bid.clone(), p, bid, number_partitions);//combined R/W executor.

            for (int k = 0; k < number_partitions; k++) {
                p_bid[p]++;
                p++;
                if (p == tthread)
                    p = 0;
            }


        } else {
//            boolean flag = read_decision[i];
//            //This ensures a balanced workload among executors.
//            cnt++;
//            if (cnt % total_children_tasks == 0) {
//                i++;
//                if (i == 8)
//                    i = 0;
//            }

            if (ccOption == CCOption_TStream) {
                if (enable_admission_control) {
                    if (control < target_Hz) {

                        collector.emit_single(bid);//combined R/W executor.

                        bid++;
                        control++;
                    } else
                        empty++;
                } else {
                    collector.emit_single(bid);//combined R/W executor.
                    bid++;
                }
                forward_checkpoint(-1, bid, null); // This is required by T-Stream.
            } else {
                collector.emit_single(bid);//combined R/W executor.

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