package spark.applications.jobs.function_spout.verbose;

import applications.common.spout.helper.DataSource;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Spark local spout only simulate data source, the wrapper is carried by another function.
 * Due to the fact that queuestream only support return one element.
 * Created by szhang026 on 5/6/2016.
 */
public class localSpout {
    private static final Logger LOG = LogManager.getLogger(localSpout.class);
    private final int skew;
    private final int tuple_size;
    private DataSource dataSource;

    public localSpout() {
        skew = 0;
        tuple_size = 8;
        dataSource = new DataSource(skew, false, tuple_size, true);
    }

    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     *
     * @param key  The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected static String getConfigKey(String key, String name, String prefix) {
        return String.format(key, String.format("%s.%s", prefix, name));
    }

    /**
     * Utility method to parse a configuration key with the application prefix..
     *
     * @param key The configuration key to be parsed
     * @return
     */
    protected static String getConfigKey(String key, String prefix) {
        return String.format(key, prefix);
    }

    public Queue<JavaRDD<String>> load_spout(JavaStreamingContext ssc, int batch) {
        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream
        Queue<JavaRDD<String>> spout = new LinkedList<JavaRDD<String>>();

        List<String> str_l = new LinkedList<String>();
        int j = 0;
        for (int m = 0; m < 10000; m++) {//10000 number of batches of in-average 100 sizes.
            for (int i = 0; i < 100; i++) {
                String msg = dataSource.generateEvent(true).getEvent();
                if (j < batch) {
                    str_l.add(msg); //normal..
                    j++;
                } else {
                    spout.add(ssc.sparkContext().parallelize(str_l));
                    j = 0;
                    str_l = new LinkedList<String>();//create the list for this batch.
                }
            }
        }
        return spout;
    }
}
