package spark.applications.jobs;

import applications.common.tools.OsUtils;
import kafka.serializer.StringDecoder;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import spark.applications.constants.microbenchmarkConstants;
import spark.applications.jobs.function_spout.SplitPair;
import spark.applications.jobs.functions.StatefulHelper;
import spark.applications.util.data.Event_MB;


public class microbenchmark extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(microbenchmark.class);
    // private transient PrintWriter outLogger = null;
    final String mode;
    final int executor_thread;
    private final int batch_duration;
    private final int window;
    private int task_type;

    public microbenchmark(String topologyName, SparkConf config) {
        super(topologyName, config);
        config.set("prefix", "mb");

        batch_duration = config.getInt("batch_duration", 1000);//in ms
        window = config.getInt("window", 1);//in s

        executor_thread = config.getInt("executor_thread", 32);
        mode = config.get("mode");
    }

    @Override
    public void initialize(JavaStreamingContext ssc) {


    }

    @Override
    public microbenchmark buildJob(JavaStreamingContext ssc) {

        /*
        * we'll make use of updateStateByKey so Spark streaming will maintain a value for every key in our dataset.
        * updateStateByKey takes in a different reduce function
        * */

        // A checkpoint is configured, or else mapWithState will complaint.
        ssc.checkpoint(System.getProperty("user.home") + OsUtils.OS_wrapper("checkpoints")); // If spark crashes, it stores its previous state in this directory, so that it can continue when it comes back online.

        /**
         * for local test purpose...
         * An extra step is required by teststream, because Spark queueStream does not allow pair stream.
         */
        JavaDStream<String> testInput;
        final JavaPairDStream<String, String> testStream;
        JavaDStream<Event_MB> inputTuples;
        final JavaPairDStream<String, Event_MB> integerTuple3JavaPairDStream;
        final boolean verbose = Boolean.parseBoolean(config.get("verbose"));

        if (mode.equalsIgnoreCase("remote")) {
            String zkQuorum = "localhost:2181";
            String brokers = "localhost:9092";
            String topics = "mb";
            Map<String, String> kafkaParams = new HashMap<>();
            Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
            kafkaParams.put("metadata.broker.list", brokers);
//            kafkaParams.put("consumer.forcefromstart", String.valueOf(true));
//            kafkaParams.put("auto.offset.reset", "smallest");
            spout = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
            kafkaStream = spout.repartition(executor_thread);
            if (verbose) {
                inputTuples = kafkaStream.flatMap(new spark.applications.jobs.function_spout.verbose.parser());
            } else {
                inputTuples = kafkaStream.flatMap(new spark.applications.jobs.function_spout.parser());
            }
        } else {

            if (verbose) {
                testInput = ssc.queueStream(new spark.applications.jobs.function_spout.verbose.localSpout().load_spout(ssc, batch));
                testStream = testInput.mapToPair(new SplitPair()).repartition(executor_thread);
                inputTuples = testStream.flatMap(new spark.applications.jobs.function_spout.verbose.parser());
            } else {
                testInput = ssc.queueStream(new spark.applications.jobs.function_spout.localSpout().load_spout(ssc, batch));
                testStream = testInput.mapToPair(new SplitPair()).repartition(executor_thread);
                inputTuples = testStream.flatMap(new spark.applications.jobs.function_spout.parser());
            }
        }

        task_type = config.getInt("task_type", 0);
        if (verbose) {
            integerTuple3JavaPairDStream = inputTuples.flatMapToPair(new spark.applications.jobs.functions.verbose.StatelessExecutor(config));
            if (task_type == 1 && window * 1000 != batch_duration) {//if batch duration is not the same as application window, we need to have special treatment.
                sink = integerTuple3JavaPairDStream.mapWithState(StateSpec.function(new spark.applications.jobs.functions.verbose.partialStatefulExecutor(config)).numPartitions(executor_thread)).mapToPair(new StatefulHelper());
            } else if (task_type == 2) {
                sink = integerTuple3JavaPairDStream.mapWithState(StateSpec.function(new spark.applications.jobs.functions.verbose.fullyStatefulExecutor(config)).numPartitions(executor_thread)).mapToPair(new StatefulHelper());
            } else {
                sink = integerTuple3JavaPairDStream;//just return..
            }
        } else {
            integerTuple3JavaPairDStream = inputTuples.flatMapToPair(new spark.applications.jobs.functions.StatelessExecutor(config));
            if (task_type == 1 && window * 1000 != batch_duration) {//if batch duration is not the same as application window, we need to have special treatment.
                sink = integerTuple3JavaPairDStream.mapWithState(StateSpec.function(new spark.applications.jobs.functions.partialStatefulExecutor(config)).numPartitions(executor_thread)).mapToPair(new StatefulHelper());
            } else if (task_type == 2) {
                sink = integerTuple3JavaPairDStream.mapWithState(StateSpec.function(new spark.applications.jobs.functions.fullyStatefulExecutor(config)).numPartitions(executor_thread)).mapToPair(new StatefulHelper());
            } else {
                sink = integerTuple3JavaPairDStream;//just return..
            }
        }
        return this;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return microbenchmarkConstants.PREFIX;
    }

}
