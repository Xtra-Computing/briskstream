package applications.spout.combo;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.TransactionalBolt;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.msgs.GeneralMsg;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.profiler.Metrics;
import org.slf4j.Logger;
import utils.SOURCE_CONTROL;

import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.content.Content.CCOption_SStore;
import static engine.content.Content.CCOption_TStream;
import static engine.profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public abstract class SPOUTCombo extends TransactionalSpout {
    private static Logger LOG;
    private static final long serialVersionUID = -2394340130331865581L;
    TransactionalBolt bolt;//compose the bolt here.
    private int the_end;
    private int global_cnt;
    protected final String split_exp = ";";
    int num_events_per_thread;

    int test_num_events_per_thread;
    long[] mybids;
    Object[] myevents;

    int counter;
    int _combo_bid_size;

    Tuple tuple;
    Tuple marker;
    GeneralMsg generalMsg;


    public SPOUTCombo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
        state = new ValueState();
    }


    @Override
    public void nextTuple() throws InterruptedException {

        try {

            if (counter == start_measure)
                if (taskId == 0)
                    bolt.sink.start();

            if (counter < test_num_events_per_thread) {
                long bid = mybids[counter];

                if (enable_latency_measurement)
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, myevents[counter % num_events_per_thread], System.nanoTime());
                else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, myevents[counter % num_events_per_thread]);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);

                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

//                LOG.info("COUNTER:" + counter);
                if (ccOption == CCOption_TStream) {// This is only required by T-Stream.
                    if (!enable_app_combo) {
                        forward_checkpoint(this.taskId, bid, null);
                    } else {
                        if (checkpoint(counter)) {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                            bolt.execute(marker);
                        }
                    }
                }

                if (counter == the_end) {
                    SOURCE_CONTROL.getInstance().Final_END(taskId);//sync for all threads to come to this line.
                    if (taskId == 0)
                        bolt.sink.end(global_cnt);
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


    public abstract void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector);

    int start_measure;

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


        num_events_per_thread = NUM_EVENTS / tthread / combo_bid_size;


        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            test_num_events_per_thread = num_events_per_thread;//otherwise deadlock.. TODO: fix it later.
            Metrics.MeasureTools.measure_counts[thisTaskId] = MeasureStart;//skip warm-up phase.
            start_measure = 0;
        } else {
            test_num_events_per_thread = TEST_NUM_EVENST / combo_bid_size;
            start_measure = MeasureStart;
        }
        counter = 0;
        mybids = new long[test_num_events_per_thread];//5000 batches.

        myevents = new Object[num_events_per_thread];

        for (int i = 0; i < test_num_events_per_thread; i++) {
            mybids[i] = thisTaskId * (combo_bid_size) + i * tthread * combo_bid_size;

        }

        if (config.getInt("CCOption", 0) == CCOption_TStream) {
            the_end = test_num_events_per_thread - test_num_events_per_thread % batch_number_per_wm;
        }else {
            the_end = test_num_events_per_thread;
        }

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;

        } else {
            global_cnt = (the_end - MeasureStart) * tthread;

        }


    }

}