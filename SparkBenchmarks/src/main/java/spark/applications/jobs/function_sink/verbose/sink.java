package spark.applications.jobs.function_sink.verbose;

import applications.common.spout.helper.Event;
import applications.common.tools.OsUtils;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import spark.applications.util.data.Event_MB;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import static applications.common.spout.helper.Event.null_expression;

/**
 * Created by I309939 on 7/21/2016.
 */
public class sink implements VoidFunction {
    private static final Logger LOG = LogManager.getLogger(sink.class);
    // final AccumulatorV2<Long, Long> processed_tuples;
    final String metric_path;
    final AccumulatorV2<Long, Long> counter;
    //    final AccumulatorV2<Double, Double> start;
//    final AccumulatorV2<Double, Double> end;
    private final int batch;
    private final long batch_duration;
    private final int runtimeInSeconds;
    // private final JavaStreamingContext ssc;
    long slowest_sink_finish = 0;

    public sink(JavaStreamingContext ssc, int batch, SparkConf sparkConf) {
        counter = ssc.sparkContext().sc().longAccumulator();
        // processed_tuples = ssc.sparkContext().sc().longAccumulator();
        batch_duration = sparkConf.getInt("batch_duration", 1);
        runtimeInSeconds = sparkConf.getInt("runtimeInSeconds", 60);//
        metric_path = sparkConf.get("metric_path", "");
        this.batch = batch;
//        start = ssc.sparkContext().sc().doubleAccumulator();
//        end = ssc.sparkContext().sc().doubleAccumulator();
        // this.ssc = ssc;
        sink_pid();
    }

    private void sink_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        //LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(metric_path + OsUtils.OS_wrapper("sink_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void call(Object o) throws Exception {
        long compute_start = System.nanoTime();
        // final List s = ((JavaRDD) o).collect();
        final List<Tuple2> collect = ((JavaPairRDD) o).collect();
        long sink_start = System.nanoTime();
        if (collect.size() > 0) {//we can tell from the collector size, how many tuples have being processed by spark in the last batch duration.
            Event_MB event = null;
            for (Tuple2 o1 : collect) {
                final Event_MB o2 = (Event_MB) o1._2();
                final String value = o2._4();
                if (!value.equals(null_expression)) {//find the first marked event in this batch
                    event = o2;
                    break;
                }
            }
            if (event != null)
                execute(compute_start, sink_start, event, collect.size());
            else
                throw new Exception("stop jobs: No marked event in this batch");
        }
    }

    private void output(Long time) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(metric_path + OsUtils.OS_wrapper("throughput.txt")));
            writer = new BufferedWriter(fw);

            writer.write(String.valueOf(time));

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void execute(long compute_start, long sink_start, Event_MB event, int size) throws Exception {

        final String value = event._4();
        counter.add(1L);
        long sink_finish = System.nanoTime();
        final String state = "EventTime"
                + Event.split_expression + event._1()
                + Event.split_expression + "compute start"
                + Event.split_expression + compute_start
                + Event.split_expression + event._4()
                + Event.split_expression + "sink"
                + Event.split_expression + sink_start
                + Event.split_expression + sink_finish;

        final long process_latency = (sink_finish - compute_start);//in ns.
        LOG.fatal("batch_size"
                + Event.split_expression + size
                + Event.split_expression + "compute delay"
                + Event.split_expression + process_latency
                + Event.split_expression + state
        );
        if (counter.value() == 50) {
            output(batch_duration);
            throw new Exception("stop jobs");
        }
    }
}
