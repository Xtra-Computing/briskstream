package applications.sink;

import applications.Constants;
import applications.bolts.lr.datatype.util.LRTopologyControl;
import applications.sink.helper.stable_sink_helper;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static applications.Constants.System_Plan_Path;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final LinkedHashMap<Long, Long> latency_map = new LinkedHashMap<>();
    private static final long serialVersionUID = 6249684803036342603L;
    protected static String directory;
    protected static String algorithm;
    protected static boolean profile = false;
    protected stable_sink_helper helper;
    protected int ccOption;
    private boolean LAST = false;

    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);

    }


    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }

    public void initialize(int task_Id_InGroup, int thisTaskId, ExecutionGraph graph) {
        super.initialize(task_Id_InGroup, thisTaskId, graph);
        int size = graph.getSink().operator.getExecutorList().size();

        String path = config.getString("metrics.output");

        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , path
                , config.getDouble("predict", 0)
                , size
                , thisTaskId
                , config.getBoolean("measure", false));
//        applications.sink.helper.helper helper2 = new stable_sink_helper(LOG
//                , config.getInt("runtimeInSeconds")
//                , metric_path, config.getDouble("predict", 0), size, thisTaskId);
        profile = config.getBoolean("profile");


        directory = System_Plan_Path + OsUtils.OS_wrapper("BriskStream")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")));
        File file = new File(directory);
        if (!file.mkdirs()) {
        }
        if (config.getBoolean("random", false)) {
            algorithm = "random";
        } else if (config.getBoolean("toff", false)) {
            algorithm = "toff";
        } else if (config.getBoolean("roundrobin", false)) {
            algorithm = "roundrobin";
        } else if (config.getBoolean("worst", false)) {
            algorithm = "worst";
        } else {
            algorithm = "opt";
        }
//		store = new ArrayDeque<>((int) 1E11);
        LAST = thisTaskId == graph.getSink().getExecutorID();
        ccOption = config.getInt("CCOption", 0);
    }

    @Override
    public void execute(Tuple input) {
        double results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            if (LAST) {
                check();
            }
        }
    }

    @Override
    public void execute(TransferTuple input) {
        //	store.add(input);
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
//			read = (input.getString(0, i));
            //simulate work..
//			dummy_execute();
            double results = helper.execute(input.getBID());
            if (results != 0) {
                this.setResults(results);
                LOG.info("Sink finished:" + results);
                if (LAST) {
                    check();
                }
            }
        }



    }

    /**
     * Only one sink will do the check.
     */
    protected void check() {
        if (!profile) {

            for (Map.Entry<Long, Long> entry : latency_map.entrySet()) {
//                LOG.info("=====Process latency of msg====");
                //LOG.DEBUG("SpoutID:" + (int) (entry.getKey() / 1E9) + " and msgID:" + entry.getKey() % 1E9 + " is at:\t" + entry.getValue() / 1E6 + "\tms");
                latency.addValue((entry.getValue() / 1E6));
            }
            try {
//                Collections.sort(col_value);

                FileWriter f = null;
                switch (algorithm) {
                    case "random": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.latency")));
                        break;
                    }
                    case "toff": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("toff.latency")));
                        break;
                    }
                    case "roundrobin": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("roundrobin.latency")));
                        break;
                    }
                    case "worst": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.latency")));
                        break;
                    }
                    case "opt": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.latency")));
                        break;
                    }
                }

                Writer w = new BufferedWriter(f);

                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(String.valueOf(latency.getPercentile(percentile) + "\n"));
                }
                w.write("=======Details=======");
                w.write(latency.toString() + "\n");
                w.close();
                f.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            LOG.info("Stop all threads sequentially");
//			context.stop_runningALL();
            context.Sequential_stopAll();
//			try {
//				//Thread.sleep(10000);
//				context.wait_for_all();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			context.force_existALL();
//			context.stop_running();
//			try {
//				Thread.sleep(10000);//wait for all sink threads stop.
//			} catch (InterruptedException e) {
//				//e.printStackTrace();
//			}
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }


}
