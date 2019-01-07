package applications.sink;

import applications.bolts.ct.TransactionResult;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class CTSink_latency extends MeasureSink_latency {
	private static final Logger LOG = LoggerFactory.getLogger(CTSink_latency.class);
	private static final long serialVersionUID = 5481794109405775823L;


	double success = 0;
	double failure = 0;

	public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
		super.initialize(thread_Id, thisTaskId, graph);
		OsUtils.configLOG(LOG);
	}

	@Override
	public void execute(Tuple input) {

		TransactionResult result = (TransactionResult) input.getValue(0);
		if (result.isSuccess()) {
			success++;
		} else {
			failure++;
		}
		double results = helper.execute(input.getBID());
		if (isSINK) {// && cnt % 1E3 == 0
			long msgId = input.getBID();
			if (msgId < max_num_msg) {
				final long end = System.nanoTime();
				final long start = input.getLong(1);
				final long process_latency = end - start;//ns
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
//				LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
				latency_map[(int) msgId] = process_latency;
//				}
				num_msg++;
			}
			if (results != 0) {
				this.setResults(results);
				LOG.info("Sink finished:" + results);
				check();
			}
		}
	}

	/**
	 * Only one sink will do the check.
	 */
	protected void check() {
		if (!profile) {

			for (int key = 0; key < max_num_msg; key++) {
//                LOG.info("=====Process latency of msg====");
				latency.addValue(latency_map[key]);
			}
			try {
//                Collections.sort(col_value);

				FileWriter f = null;

				f = new FileWriter(new File(metric_path + OsUtils.OS_wrapper("latency")));

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

	public void display() {
		LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
		LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
	}
}
