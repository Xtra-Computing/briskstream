package applications.sink;

import applications.Constants;
import applications.datatype.util.LRTopologyControl;
import applications.sink.helper.stable_sink_helper;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.JumboTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static applications.CONTROL.*;
import static applications.Constants.STAT_Path;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    protected static final LinkedHashMap<Long, Long> latency_map = new LinkedHashMap<>();
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
        ccOption = config.getInt("CCOption", 0);

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


        directory = STAT_Path + OsUtils.OS_wrapper("BriskStream")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket"))
                + OsUtils.OS_wrapper(String.valueOf(ccOption)))
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("checkpoint")))
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("theta")))
        ;

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

    }

    int cnt = 0;

    @Override
    public void execute(Tuple input) {
        check(cnt, input);
//        LOG.info("CNT:" + cnt);
        cnt++;
    }

//    @Override
//    public void execute(JumboTuple input) {
//        //	store.add(input);
//        int bound = input.length;
//        for (int i = 0; i < bound; i++) {
////			read = (input.getString(0, i));
//            //simulate work..
////			dummy_execute();
//            double results = helper.execute(input.getBID());
//            if (results != 0) {
//                this.setResults(results);
//                LOG.info("Sink finished:" + results);
//                if (LAST) {
//                    measure_end();
//                }
//            }
//        }
//
//    }

    protected void check(int cnt, Tuple input) {
        if (cnt == 0) {
            helper.StartMeasurement();
        } else if (cnt == (NUM_EVENTS - 40 * 10 - 1)) {
            double results = helper.EndMeasurement(cnt);
            this.setResults(results);
            if (!enable_engine)//performance measure for TStream is different.
                LOG.info("Received:" + cnt + " throughput:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end();
            }
        }
        if (enable_latency_measurement)
            if (isSINK) {// && cnt % 1E3 == 0
                long msgId = input.getBID();
                if (msgId < max_num_msg) {
                    final long end = System.nanoTime();

//                    try {
                    final long start = input.getLong(1);


                    final long process_latency = end - start;//ns
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
//				LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                    latency_map.put(msgId, process_latency);
//				}
//                    } catch (Exception e) {
//                        System.nanoTime();
//                    }
//                    num_msg++;
                }
            }
    }

    /**
     * Only one sink will do the measure_end.
     */
    protected void measure_end() {
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
                w.write("===90th===" + "\n");
                w.write(String.valueOf(latency.getPercentile(90) + "\n"));
                w.close();
                f.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            LOG.info("Stop all threads sequentially");
//			context.stop_runningALL();
            context.Sequential_stopAll();

        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }


}
