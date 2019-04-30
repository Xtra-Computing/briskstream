package applications.spout.gs;

import applications.bolts.mb.*;
import applications.spout.MicroBenchmarkSpout;
import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import brisk.faulttolerance.impl.ValueState;
import brisk.util.BID;
import engine.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.NUM_EVENTS;
import static applications.CONTROL.combo_bid_size;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.content.Content.*;
import static engine.profiler.Metrics.NUM_ITEMS;

public class GSCombo extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBenchmarkSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;


    MBBolt bolt;//compose the bolt here.


    public GSCombo() {
        super(LOG);
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

        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);


        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new Bolt_nocc(0);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new Bolt_olb(0);
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new Bolt_lwm(0);
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new Bolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new Bolt_sstore(0);
                break;
            }
        }
    }


    @Override
    public void nextTuple() throws InterruptedException {

        try {
//            if (ccOption == CCOption_TStream)
//                forward_checkpoint(-1, bid, null); // This is only required by T-Stream.

            int bid = BID.getInstance().get();

            if (bid < NUM_EVENTS) {

                for (int i = bid; i < bid + combo_bid_size; i++) {
                    bolt.execute(new Tuple(i, this.taskId, context, new GeneralMsg<>(DEFAULT_STREAM_ID, System.nanoTime())));  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                }

            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }
}