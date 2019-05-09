package applications.spout.combo;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.TransactionalBolt;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import org.slf4j.Logger;

import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.content.Content.CCOption_TStream;
import static engine.profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class Combo extends TransactionalSpout {
    private static Logger LOG;
    private static final long serialVersionUID = -2394340130331865581L;
    TransactionalBolt bolt;//compose the bolt here.

    public Combo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
        state = new ValueState();
    }


    int num_batch;
    long[] mybids;
    int counter;
    int _combo_bid_size;

    @Override
    public void nextTuple() throws InterruptedException {

        try {

            if (counter == 0)
                bolt.sink.start();

            if (counter < num_batch) {
                long bid = mybids[counter];//SOURCE_CONTROL.getInstance().GetAndUpdate();
                bolt.execute(new Tuple(bid, this.taskId, context,
                        new GeneralMsg<>(DEFAULT_STREAM_ID, System.nanoTime())));  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_TStream) {// This is only required by T-Stream.
                    if (!enable_app_combo) {
                        forward_checkpoint(this.taskId, bid, null);
                    } else {
                        if (checkpoint(counter)) {
                            bolt.execute(new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, System.nanoTime(), bid, myiteration)));
//                        success = true;
                        }
                    }
                }
            } else if (counter == num_batch) {//the last one need to force emit a watermark.

                if (ccOption == CCOption_TStream) {// This is only required by T-Stream.
                    if (!enable_app_combo) {
                        forward_checkpoint(this.taskId, bid, null);
                    } else {
                        bolt.execute(new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, System.nanoTime(), bid, myiteration)));
//                        success = true;
                    }
                }

            }
        } catch (DatabaseException | BrokenBarrierException e) {
            //e.printStackTrace();
        }
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

        double checkpoint = config.getDouble("checkpoint", 1);

        batch_number_per_wm = (int) (10000 * checkpoint);//10K, 1K, 100.

        LOG.info("batch_number_per_wm (watermark events length)= " + (batch_number_per_wm) * combo_bid_size);


        num_batch = NUM_EVENTS / tthread / combo_bid_size;

        mybids = new long[num_batch];//5000 batches.

        for (int i = 0; i < num_batch; i++) {
            mybids[i] = thisTaskId * (combo_bid_size) + i * tthread * combo_bid_size;
        }
        counter = 0;
    }

}