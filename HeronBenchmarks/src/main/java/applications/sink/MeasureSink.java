package applications.sink;

import constants.BaseConstants;
import helper.helper;
import helper.stable_sink_helper;
import util.OsUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    helper helper;
    helper helper2;
    int processed1 = 0;
    int processed2 = 0;
    int size = 1;
    int tn = 0, an = 0, ab = 0, de = 0;
    String throughput_path;
    private long end;
    private long start;
    private boolean helper_finished = false;
    private boolean helper2_finished = false;
    private boolean profile = false;
    private boolean report = false;

    public MeasureSink() {
    }

    public void initialize() {
        size = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
        String path = config.getString("metrics.output") + "/"
                + config.getInt("num_socket") + "_"
                + String.valueOf(config.getInt("tthread"))
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
        throughput_path = path + OsUtils.OS_wrapper("finalthroughput.txt");
    }

    void output(double time) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(throughput_path));
            writer = new BufferedWriter(fw);
            writer.write(String.valueOf(time));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        double results = helper.execute();
        if (results != 0) {
            LOG.info("Sink finished:" + results);
//            this.context.setTaskData("results", results);
        }
        if (results != 0) {
            output(results);
//            System.out.println("finished measurement (k events/s):" + results * size + ")");
//            Thread.currentThread().stop();
            killTopology();
//            System.exit(0);

        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
