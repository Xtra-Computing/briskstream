package applications.general.sink;

import applications.constants.BaseConstants;
import applications.general.sink.helper.helper;
import applications.general.sink.helper.stable_sink_helper;
import applications.util.OsUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashMap;

import static applications.Constants.System_Plan_Path;

public class MeasureSink extends BaseSink {
	private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
	helper helper;
	helper helper2;
	int processed1 = 0;
	int processed2 = 0;
	int size = 1;
	int tn = 0, an = 0, ab = 0, de = 0;
	private long end;
	private long start;
	private boolean helper_finished = false;
	private boolean helper2_finished = false;
	private boolean profile = false;

	private LinkedHashMap<Long, Long> latency_map = new LinkedHashMap<>();
	DescriptiveStatistics latency = new DescriptiveStatistics();
	String directory;


	public MeasureSink() {
	}

	public void initialize() {
		size = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
		String output_path = config.getString("metrics.output") + "/"
				+ config.getInt("num_socket")
				+ "_" + String.valueOf(config.getInt("tthread"))
				+ "_" + String.valueOf(config.getInt("parallelism"))
//                + "_"
//                + String.valueOf(config.getInt("pthread")) + "_"
//                + String.valueOf(config.getInt("ct1")) + "_"
//                + String.valueOf(config.getInt("ct2"))
				;

		helper = new stable_sink_helper(LOG
				, config.getInt("runtimeInSeconds")
				, output_path, config.getDouble("predict", 0), size, context.getThisTaskId());
		helper2 = new stable_sink_helper(LOG
				, config.getInt("runtimeInSeconds")
				, output_path, config.getDouble("predict", 0), size, context.getThisTaskId());
		profile = config.getBoolean("profile");

		LOG.info("#SINK:" + size);


		String Plan_Path = System_Plan_Path + OsUtils.OS_wrapper("Flink");
		directory = Plan_Path
				+ OsUtils.OS_wrapper(configPrefix)
				+ OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")))
				+ OsUtils.OS_wrapper(String.valueOf(config.getInt("tthread")))
				+ OsUtils.OS_wrapper(String.valueOf(config.getInt("parallelism")));
		File file = new File(directory);
		if (!file.mkdirs()) {
		}
	}

	@Override
	public void execute(Tuple input) {
		double results = helper.execute();


		if (results != 0) {
			LOG.info("Sink finished:" + results);
			System.out.println("finished measurement (k events/s):" + results * size + ")");

			try {
				FileWriter f = new FileWriter(new File(directory
						+ OsUtils.OS_wrapper("flink.throughput")));
				Writer w = new BufferedWriter(f);
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
