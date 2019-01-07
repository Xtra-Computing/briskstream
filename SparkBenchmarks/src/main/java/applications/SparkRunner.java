package applications;

import applications.common.abstractRunner;
import applications.common.tools.OsUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.applications.jobs.AbstractJob;
import spark.applications.jobs.microbenchmark;
import spark.applications.util.propertiesUtil;

public class SparkRunner extends abstractRunner {
    private static final org.slf4j.Logger LOG = LogManager.getLogger(SparkRunner.class);
    private final AppDriver driver;
    @Parameter(names = {"-mf"}, description = "memoryFraction", required = false)
    public double memoryFraction = 0.6;
    @Parameter(names = {"-ma", "--master"}, description = "URL of master", required = false)
    public String URL_master = "spark://localhost:7077";
    @Parameter(names = {"-bt_duration"}, description = "Spark-streaming batch duration", required = false)
    private long batch_duration = 2000;//1,2 or 4.

    public SparkRunner() {
        driver = new AppDriver();
        driver.addApp("microbenchmark", microbenchmark.class);
    }

    public static void main(String[] args) throws Exception {
//        URL location = SparkRunner.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(location.getFile());

        SparkRunner runner = new SparkRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
//            System.exit(1);
        }
        runner.run();
    }


    public void run() throws InterruptedException {


        // load configuration
        SparkConf sparkConf = propertiesUtil.load(application);

        configuration(sparkConf);

        String log4jConfPath;

        String log = sparkConf.get("log");
        if (mode.equalsIgnoreCase("remote"))
            if (OsUtils.isWindows()) {
                log4jConfPath = "C:\\Users\\I309939\\Documents\\NUMA-streamBenchmarks\\common\\src\\main\\resources" + OsUtils.OS_wrapper(log.concat(".properties"));
            } else {
                log4jConfPath = System.getProperty("user.home").concat("/NUMA-streamBenchmarks/common/src/main/resources" + OsUtils.OS_wrapper(log.concat(".properties")));
            }
        else {
            if (OsUtils.isWindows()) {
                log4jConfPath = "C:\\Users\\I309939\\Documents\\NUMA-streamBenchmarks\\common\\src\\main\\resources" + OsUtils.OS_wrapper(log.concat(".properties"));
            } else {
                log4jConfPath = System.getProperty("user.home").concat("/NUMA-streamBenchmarks/common/src/main/resources" + OsUtils.OS_wrapper(log.concat(".properties")));
            }
        }
        PropertyConfigurator.configure(log4jConfPath);

        sparkConf.set("executor_thread", String.valueOf(threads1));
        sparkConf.set("batch_duration", String.valueOf(batch_duration));
        sparkConf.setAppName("Spark-App");
        sparkConf.setJars(JavaStreamingContext.jarOfClass(SparkRunner.class));
        sparkConf.set("spark.storage.memoryFraction", String.valueOf(memoryFraction));

        switch (mode) {
            case RUN_LOCAL: {
                sparkConf.setMaster("local[*]");
                break;
            }
            case RUN_REMOTE: {
                sparkConf.setMaster(URL_master);
                break;
            }
        }

        // Create the context
        LOG.fatal(sparkConf.toDebugString());
        final Duration duration = new Duration(batch_duration);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, duration);

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // Get the application and execute on Spark
        AbstractJob job = app.getJob(ssc, application, sparkConf);
        //result.print();
        final boolean verbose = Boolean.parseBoolean(sparkConf.get("verbose"));

        LOG.fatal("Run spark in verbose mode?" + verbose);
        if (verbose) {
            job.sink.foreachRDD(new spark.applications.jobs.function_sink.verbose.sink(ssc, batch, sparkConf));
        } else {
            job.sink.foreachRDD(new spark.applications.jobs.function_sink.sink(ssc, batch, sparkConf));
        }
        /**
         * In general, we should implement "sink" here, as this is where the final results are obtained.
         */
//
//        //make all RDD to share the same key in order to maintain global states for sink purpose..
//        final JavaPairDStream javaPairDStream = result.mapToPair((PairFunction) o -> new Tuple2<>("mykey", o));
//
//        StateSpec ss = StateSpec.function(new warmupstatefulSink()).initialState(job.spout);
//        final JavaMapWithStateDStream javaMapWithStateDStream = javaPairDStream.mapWithState(ss);
//        javaMapWithStateDStream.print();


        ssc.start();
        ssc.awaitTermination();
        // output(0);//just to inform the test script, it has completed Brisk.execution.
    }


}
