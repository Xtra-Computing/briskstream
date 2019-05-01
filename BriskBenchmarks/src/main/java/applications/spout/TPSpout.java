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

import java.util.Random;

import static applications.CONTROL.*;
import static engine.content.Content.CCOption_TStream;
import static engine.profiler.Metrics.NUM_ITEMS;

public class TPSpout extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TPSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;

    int i = 0;
    int cnt = 0;
    int j = 0;

    private Random r = new Random();

    public TPSpout() {
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

        for (TopologyComponent children : this.context.getThisComponent().getChildrenOfStream().keySet()) {
            int numTasks = children.getNumTasks();
            total_children_tasks += numTasks;
        }

        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);

        load_input();
    }


    void control_emit() throws InterruptedException {
        if (control < target_Hz && success) {

            if (enable_latency_measurement)
                collector.emit_single(bid++, array_array[counter], System.nanoTime());//combined R/W executor.
            else
                collector.emit_single(bid++, array_array[counter]);//combined R/W executor.
            control++;
            counter++;
            if (counter == array_array.length) {
                counter = 0;
            }
        } else
            empty++;
    }

    @Override
    public void nextTuple() throws InterruptedException {
        if (ccOption == CCOption_TStream)
            forward_checkpoint(-1, bid, null); // This is only required by T-Stream.

//        if (bid < NUM_EVENTS) {
            if (ccOption == CCOption_TStream && enable_admission_control) {
                control_emit();
            } else {
                if (enable_latency_measurement)
                    collector.emit(bid++, array_array[counter], System.nanoTime());//combined R/W executor.
                else
                    collector.emit(bid++, array_array[counter]);//combined R/W executor.
            }
            counter++;
            if (counter == array_array.length) {
                counter = 0;
            }
//        }
    }


    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {

    }

}