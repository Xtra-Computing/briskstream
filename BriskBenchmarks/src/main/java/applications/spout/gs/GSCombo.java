package applications.spout.gs;

import applications.bolts.mb.*;
import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SOURCE_CONTROL;

import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.content.Content.*;
import static engine.profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class GSCombo extends TransactionalSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GSCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;


    GSBolt bolt;//compose the bolt here.


    public GSCombo() {
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

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);


        double checkpoint = config.getDouble("checkpoint", 1);
        batch_number_per_wm = (int) (200 * checkpoint);//Math.max(10, (int) (MIN_EVENTS_PER_THREAD * checkpoint));//only for TSTREAM.
        LOG.info("batch_number_per_wm (watermark events length)= " + (batch_number_per_wm) * combo_bid_size);
    }


    @Override
    public void nextTuple() throws InterruptedException {

        try {
            if (ccOption == CCOption_TStream) {// This is only required by T-Stream.
                if (!enable_app_combo)
                    forward_checkpoint(this.taskId, bid, null);

                else {
                    if (checkpoint()) {
                        bolt.execute(new Tuple(-1, this.taskId, context, new Marker(DEFAULT_STREAM_ID, System.nanoTime(), -1, myiteration)));
//                        success = true;
                    }
                }
            }

            long bid = SOURCE_CONTROL.getInstance().GetAndUpdate();

            if (bid < NUM_EVENTS) {
                bolt.execute(new Tuple(bid, this.taskId, context,
                        new GeneralMsg<>(DEFAULT_STREAM_ID, System.nanoTime())));  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            } else {

            }
        } catch (DatabaseException | BrokenBarrierException e) {
            //e.printStackTrace();
        }
    }
}