package applications.sink;

import constants.BaseConstants;
import helper.helper;
import helper.stable_sink_helper;
import util.OsUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static applications.Constants.System_Plan_Path;
import static constants.BaseConstants.BaseField.MSG_ID;

public class MeasureSink_latency extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink_latency.class);
    helper helper;
    helper helper2;
    int processed1 = 0;
    int processed2 = 0;
    int size = 1;
    int tn = 0, an = 0, ab = 0, de = 0;
    DescriptiveStatistics latency = new DescriptiveStatistics();
    String directory;
    private long end;
    private long start;
    private boolean helper_finished = false;
    private boolean helper2_finished = false;
    private boolean profile = false;
    private boolean report = false;
    private LinkedHashMap<Long, Long> latency_map = new LinkedHashMap<>();

    public MeasureSink_latency() {
    }

    public void initialize() {
        size = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
        String path = config.getString("metrics.output") + "/"
                + config.getInt("num_socket")
                + "_" + String.valueOf(config.getInt("tthread"))
//                + "_"
//                + String.valueOf(config.getInt("pthread")) + "_"
//                + String.valueOf(config.getInt("ct1")) + "_"
//                + String.valueOf(config.getInt("ct2"))
                ;

        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , path, config.getDouble("predict", 0), size, context.getThisTaskId());
        helper2 = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , path, config.getDouble("predict", 0), size, context.getThisTaskId());
        profile = config.getBoolean("profile");

        LOG.info("#SINK:" + size);

        directory = System_Plan_Path + OsUtils.OS_wrapper("Storm")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")))
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("tthread")));
        File file = new File(directory);
        if (!file.mkdirs()) {
        }

    }

    @Override
    public void execute(Tuple input) {
        double results = helper.execute();


        if (input.getLongByField(MSG_ID) != -1) {
            final long end = System.nanoTime();
            final long start = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);
            final long process_latency = end - start;
//				final Long stored_process_latency = latency_map.getOrDefault(input.getLongByField(MSG_ID), 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
            latency_map.put(input.getLongByField(MSG_ID), process_latency);
//				}
        }

        if (!latency_map.isEmpty() && results != 0) {
            LOG.info("Sink finished:" + results);
            System.out.println("finished measurement (k events/s):" + results * size + ")");
            for (Map.Entry<Long, Long> entry : latency_map.entrySet()) {
//                LOG.info("=====Process latency of msg====");
                // //LOG.DEBUG("SpoutID:" + (int) (entry.getKey() / 1E9) + " and msgID:" + entry.getKey() % 1E9 + " is at:\t" + entry.getValue() / 1E6 + "\tms");
                latency.addValue((entry.getValue() / 1E6));
            }
            try {
                FileWriter f = new FileWriter(new File(directory
                        + OsUtils.OS_wrapper("storm.latency")));
                Writer w = new BufferedWriter(f);

                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(String.valueOf(latency.getPercentile(percentile) + "\n"));
                }

                w.write("=======Details=======");
                w.write(latency.toString() + "\n");

                w.close();
                f.close();

                f = new FileWriter(new File(directory
                        + OsUtils.OS_wrapper("storm.throughput")));
                w = new BufferedWriter(f);
                w.write(String.valueOf(results * size + "\n"));

                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            killTopology();

        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
