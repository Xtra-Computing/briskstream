package spark.applications.jobs.function_sink;

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

/**
 * Created by I309939 on 7/21/2016.
 */
public class sink_withcounter implements VoidFunction {
    private static final Logger LOG = LogManager.getLogger(sink_withcounter.class);
    final AccumulatorV2<Long, Long> counter;
    // final AccumulatorV2<Long, Long> processed_tuples;
    final String metric_path;
    //    final AccumulatorV2<Double, Double> start;
//    final AccumulatorV2<Double, Double> end;
    private final int batch;
    private final long batch_duration;
    private final int runtimeInSeconds;
    // private final JavaStreamingContext ssc;
    private final boolean with_counter = true;

    public sink_withcounter(JavaStreamingContext ssc, int batch, SparkConf sparkConf) {
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

    void sink_pid() {
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
        long receive_time = System.nanoTime();
        // final List s = ((JavaRDD) o).collect();
        final List collect = ((JavaPairRDD) o).collect();
        if (collect.size() > 0) {//we can tell from the collector size, how many tuples have being processed by spark in the last batch duration.
            final Tuple2 o1 = (Tuple2) collect.get(0);
            final Event_MB o2 = (Event_MB) o1._2();
            execute(receive_time, counter, o2, collect.size());
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

    private void execute(long receive_time, AccumulatorV2<Long, Long> counter, Event_MB event, int size) throws Exception {
        if (with_counter)
            counter.add(1L);
        //processed_tuples.add(new Long(size));//spark finishes a batch of tuples in the last duration (1, 2, 4 seconds).
        long sink_finish = System.nanoTime();
        final String state = "EventTime"
                + Event.split_expression + event._1()
                + Event.split_expression + event._4()
                + Event.split_expression + "sink"
                + Event.split_expression + receive_time
                + Event.split_expression + sink_finish;
        //final String[] split = state.split(Event.split_expression);
        final long process_latency = (sink_finish - event._2()) / 1000000;//reverse Brisk.execution
        //long event_latency = (receive_time - event._1()) / 1000000;//in MS
        //if (counter.value() % 5 == 0) {//to reduce the overhead of output.. we only report sample throughput per 5 batches (i.e., 5, 10, 20 seconds once).
        LOG.fatal("batch_size"
                + Event.split_expression + size
                + Event.split_expression + "throuhgput"
                + Event.split_expression + String.format("%.2f", (double) size / (process_latency))
                + Event.split_expression + state
        );
        // processed_tuples.reset();//reset the Brisk.execution.runtime.tuple processed.

        if (with_counter)
            if (Math.max(counter.value() * batch_duration, counter.value() * process_latency) > runtimeInSeconds * 1000) {
                //ssc.stop();
                output(batch_duration);
                throw new Exception("stop jobs");
            }

        //}

//        if (accum.value() % 100 == 0) {//every time Spark finishes processing a whole batch.. We output per 100 batches.
//            final double v = batch * 1000000.0 / (end.value() - start.value());
//            LOG.fatal("Average Throuhgput:" + v + "o1 timestamp:" + o1._1());
//            if (accum.value() == 10000) {
//                System.exit(0);
//            }
//        }

    }
}
