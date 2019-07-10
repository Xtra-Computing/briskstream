package applications.general.sink;

import applications.general.sink.helper.stable_sink_helper;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class ReadSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ReadSink.class);
    private static final long serialVersionUID = -4676068063417404087L;
    int processed1 = 0;
    int processed2 = 0;
    private stable_sink_helper helper;
    private double dummy = 0;
    private int complexity;
    private long end;
    private long start;
    private boolean helper_finished = false;
    private boolean helper2_finished = false;
    private LinkedList<String> store = new LinkedList<>();
    private boolean profile = false;

    private ReadSink() {
        super(LOG);
    }

    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict"), 0, thread_Id, false);
        stable_sink_helper helper2 = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict"), 0, thread_Id, false);
        profile = config.getBoolean("profile");

        complexity = config.getInt("I_C");
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        if (profile) {
//
//            String string = in.getString(0);
//            Store.add(string);
//            if (stat != null) stat.start_measure();
            for (int i = 0; i < complexity; i++) {
                double random = Math.random();
                dummy += random;
            }
//            LOG.info(string);
            helper.execute(in.getBID());
//            if (stat != null) stat.end_measure();

        } else {
            for (int i = 0; i < complexity; i++) {
                double random = Math.random();
                dummy += random;
            }
            //other mode waits for running sufficiently long time.
            if (helper.execute(in.getBID()) != 0) {
                this.getContext().stop_runningALL();
                try {
                    LOG.info("Stop all other threads except sink");
                    //Thread.sleep(10000);
                    this.getContext().wait_for_all();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.getContext().stop_running();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ignored) {
                    //e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            if (profile) {
//
//            String string = in.getString(0);
//            Store.add(string);
//            if (stat != null) stat.start_measure();
                for (int _i = 0; _i < complexity; _i++) {
                    double random = Math.random();
                    dummy += random;
                }
//            LOG.info(string);
                helper.execute(in.getBID());
//            if (stat != null) stat.end_measure();

            } else {
                for (int _i = 0; _i < complexity; _i++) {
                    double random = Math.random();
                    dummy += random;
                }
                //other mode waits for running sufficiently long time.
                if (helper.execute(in.getBID()) != 0) {
                    this.getContext().stop_runningALL();
                    try {
                        LOG.info("Stop all other threads except sink");
                        //Thread.sleep(10000);
                        this.getContext().wait_for_all();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    this.getContext().stop_running();
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ignored) {
                        //e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
