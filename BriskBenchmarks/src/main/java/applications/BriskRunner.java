package applications;

import applications.constants.*;
import applications.topology.*;
import applications.topology.latency.LinearRoad_latency;
import applications.topology.latency.WordCount_latency;
import applications.util.Configuration;
import applications.util.Constants;
import applications.util.OsUtils;
import brisk.components.Topology;
import brisk.components.TopologyComponent;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.executorThread;
import brisk.topology.TopologySubmitter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.Properties;

import static applications.Constants.System_Plan_Path;
import static applications.constants.CrossTableConstants.Conf.CT_THREADS;
import static applications.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static applications.constants.PositionKeepingConstants.Conf.PK_THREADS;
import static applications.constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;

public class BriskRunner extends abstractRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BriskRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private applications.Platform p;


    private BriskRunner() {
        driver = new AppDriver();
        driver.addApp("StreamingAnalysis", StreamingAnalysis.class);//Extra
        driver.addApp("WordCount", WordCount.class);
        driver.addApp("FraudDetection", FraudDetection.class);
        driver.addApp("SpikeDetection", SpikeDetection.class);
        driver.addApp("TrafficMonitoring", TrafficMonitoring.class);
        driver.addApp("LogProcessing", LogProcessing.class);
        driver.addApp("VoIPSTREAM", VoIPSTREAM.class);
        driver.addApp("LinearRoad", LinearRoad.class);//

        //test latency
        driver.addApp("WordCount_latency", WordCount_latency.class);
        driver.addApp("LinearRoad_latency", LinearRoad_latency.class);//

        
    }

    public static void main(String[] args) {

        BriskRunner runner = new BriskRunner();
        JCommander cmd = new JCommander(runner);

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        try {
            runner.run();
        } catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
        }
    }

    private static double runTopologyLocally(Topology topology, Configuration conf) throws InterruptedException {
        TopologySubmitter submitter = new TopologySubmitter();
        final_topology = submitter.submitTopology(topology, conf);
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();

        long start = System.currentTimeMillis();
        sinkThread.join((long) (30 * 1E3 * 60));//wait for sink thread to stop. Maximally wait for 10 mins

        long time_elapsed = (long) ((System.currentTimeMillis() - start) / 1E3 / 60);//in mins

        if (time_elapsed > 20) {
            LOG.info("Program error, exist...");
            System.exit(-1);
        }

        if (conf.getBoolean("simulation")) {
            System.exit(0);
        }
        submitter.getOM().join();
        submitter.getOM().getEM().exist();
        if (sinkThread.running) {
            LOG.info("The application fails to stop normally, exist...");
            return -1;
        } else {
            TopologyComponent sink = submitter.getOM().g.getSink().operator;
            double sum = 0;
//			double pre_results = sinkThread.getResults();
            int cnt = 0;
            for (ExecutionNode e : sink.getExecutorList()) {
                double results = e.op.getResults();
                if (results != 0) {
//					pre_results = results;
                    sum += results;
                } else {
                    sum += sum / cnt;
                }
                cnt++;
            }
            return sum;
        }
    }

    private void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default
        // configuration
        // load default configuration
        if (configStr == null) {

            String cfg = String.format(CFG_PATH, application);
            Properties p = null;
            try {
                p = loadProperties(cfg);
            } catch (IOException e) {
                e.printStackTrace();
            }

            config.putAll(Configuration.fromProperties(p));
            if (mode.equalsIgnoreCase(RUN_REMOTE)) {
                final String spout_class = String.valueOf(config.get("mb.spout.class"));
                if (spout_class.equals("applications.spout.LocalStateSpout")) {
                    LOG.info("Please use kafkaSpout in cluster mode!!!");
                    System.exit(-1);
                }
            }

            config.put(Configuration.TOPOLOGY_WORKER_CHILDOPTS, CHILDOPTS);

            configuration(config);

            switch (config.getInt("machine")) {
                case 0:
                    this.p = new applications.HUAWEI_Machine();
                    break;
                case 1:
                    this.p = new applications.HP_Machine();
                    break;
                case 2:
                    this.p = new applications.HP_Machine();
                    break;
                default:
                    this.p = new applications.RTM_Machine();
            }

            if (simulation) {
                LOG.info("Simulation: use machine:" + config.getInt("machine")
                        + " with sockets:" + config.getInt("num_socket")
                        + " and cores:" + config.getInt("num_cpu"));
            }

            //configure database.

            int max_hz = 0;
            boolean profile = config.getBoolean("profile");
            //  boolean benchmark = config.getBoolean("benchmark");
            //configure threads.
            int tthread = config.getInt("tthread");

            config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);
            config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);
            config.put(BaseConstants.BaseConf.PARSER_THREADS, pthread);
            //set total parallelism, equally parallelism
            switch (application) {
                case "CrossTables": {
                    config.put("app", 1);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(CT_THREADS, threads);
                    break;
                }
                case "OnlineBiding": {
                    config.put("app", 2);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(OB_THREADS, threads);
                    break;
                }

                case "PositionKeeping": {
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(PK_THREADS, threads);
                    break;
                }

                case "MicroBenchmark": {
                    config.put("app", 0);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(MicroBenchmarkConstants.Conf.Executor_Threads, threads);
//                    double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
//                    int r_threads = (int) (threads * ratio_of_read);
//                    int w_threads = threads - r_threads;
//
//                    config.put(MicroBenchmarkConstants.Conf.SELECTOR_THREADS, r_threads);
//                    config.put(MicroBenchmarkConstants.Conf.INSERTOR_THREADS, w_threads);
                    break;
                }
                case "StreamingAnalysis": {
                    int threads = (int) Math.floor(tthread / 5.0);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS1, threads);
                    config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS2, threads);
                    config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS3, threads);
                    config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS4, threads);
                    break;
                }
                case "WordCount": {

                    if (profile) {//profile under varying replication setting.
                        int threads = tthread;
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, threads);
                    } else {
                        int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 3.0));
                        LOG.info("Average threads:" + threads);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);//insignificant
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, threads);//2
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, threads);
                    }
                    max_hz = WordCountConstants.max_hz;
                    break;
                }
                case "WordCount_FT": {
                    if (profile) {

                    } else {
                        int threads = (int) Math.floor((tthread - sthread - sithread) / 3.0);
                        LOG.info("Average threads:" + threads);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);//insignificant
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, threads);//2
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, threads);
                    }
                    max_hz = WordCountConstants.max_hz;
                    break;
                }
                case "FraudDetection": {
                    //config.put(BaseConstants.BaseConf.SPOUT_THREADS, 20);//special treatment to FD>
                    //config.put(BaseConstants.BaseConf.SPOUT_THREADS, 16);//special treatment to FD>

                    if (profile) {
                        int threads = (int) Math.floor(tthread);
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads);
                    } else {
                        int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 2.0));
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);//insignificant
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads);
                    }
                    max_hz = FraudDetectionConstants.max_hz;
                    break;
                }
                case "SpikeDetection": {
                    //config.put(BaseConstants.BaseConf.SPOUT_THREADS, 1);//special treatment to SD>
                    if (profile) {
                        int threads = (int) Math.floor(tthread);
                        config.put(MOVING_AVERAGE_THREADS, threads);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads);//insignificant
                    } else {
                        int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 3.0));
                        LOG.info("Average threads:" + threads);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                        config.put(MOVING_AVERAGE_THREADS, threads);//insignificant
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads);//insignificant
                    }
                    max_hz = SpikeDetectionConstants.max_hz;
                    break;
                }

                case "LogProcessing": {
                    config.put(BaseConstants.BaseConf.SPOUT_THREADS, 1);//special treatment to LG>
                    int threads = (int) Math.floor(tthread / 5.0);
                    LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, threads);//2
                    config.put(LogProcessingConstants.Conf.GEO_STATS_THREADS, threads);//insignificant
                    config.put(LogProcessingConstants.Conf.STATUS_COUNTER_THREADS, threads);//insignificant
                    config.put(LogProcessingConstants.Conf.VOLUME_COUNTER_THREADS, threads);//insignificant
                    break;
                }
                case "VoIPSTREAM": {
                    int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 11.0));
                    LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.RCR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.ECR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.ENCR_THREADS, threads);//insignificant
                    config.put(VoIPSTREAMConstants.Conf.CT24_THREADS, threads);//insignificant
                    config.put(VoIPSTREAMConstants.Conf.ECR24_THREADS, threads);
                    //   config.put(VoIPSTREAMConstants.Conf.GLOBAL_ACD, threads); 1
                    config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.URL_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.ACD_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.SCORER_THREADS, threads);
                    break;
                }
                case "LinearRoad": {
                    int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 10.0));
                    //LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(LinearRoadConstants.Conf.DispatcherBoltThreads, threads);
                    config.put(LinearRoadConstants.Conf.AccidentDetectionBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.COUNT_VEHICLES_Threads, threads);//insignificant
                    //config.put(LinearRoadConstants.Conf.dailyExpBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.AccidentNotificationBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_cv_BoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_las_BoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_pos_BoltThreads, threads);//insignificant
                    //config.put(LinearRoadConstants.Conf.AccountBalanceBoltThreads, threads);
                    config.put(LinearRoadConstants.Conf.AverageSpeedThreads, threads);
                    config.put(LinearRoadConstants.Conf.LatestAverageVelocityThreads, threads);
                    break;
                }
            }
            Constants.default_sourceRate = config.getInt("targetHz");
        } else {
            config.putAll(Configuration.fromStr(configStr));
        }

        DescriptiveStatistics record = new DescriptiveStatistics();

        System.gc();
        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }

        // Get the topology
        Topology topology = app.getTopology(topologyName, config);
        topology.addMachine(p);


//        for (TopologyComponent topologyComponent : topologyComponents) {
//            if (topologyComponent.getFID() != -1) {
//                metrics.initilize(topologyComponent.getId(), config.getInt("NUM_ACCESS", 0));
//
//                for (int task : topologyComponent.getExecutorIDList()) {
//                    metrics.initilize(task);
//                }
//            }
//        }

        // Run the topology
        double rt = runTopologyLocally(topology, config);

        Collection<TopologyComponent> topologyComponents = final_topology.getRecords().values();

        if (rt != -1) {//returns normally.
            record.addValue(rt);
        }
        LOG.info("Bounded throughput (k events/s):" + config.getDouble("bound", 0));
        LOG.info("predict throughput (k events/s):" + config.getDouble("predict", 0));
        LOG.info("finished measurement (k events/s):" + record.getPercentile(50) + "("
                + (Math.abs(record.getPercentile(50)
                - config.getDouble("predict", 0))
                / config.getDouble("predict", 0)) + ")");

        String algorithm;
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

        String directory = System_Plan_Path + OsUtils.OS_wrapper("BriskStream")
                + OsUtils.OS_wrapper(topology.getPrefix())
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")));

        File file = new File(directory);
        if (!file.mkdirs()) {
        }

        FileWriter f = null;

        try {
            switch (algorithm) {
                case "random": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.throughput")));
                    break;
                }
                case "toff": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("toff.throughput")));
                    break;
                }
                case "roundrobin": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("roundrobin.throughput")));
                    break;
                }
                case "worst": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.throughput")));
                    break;
                }
                case "opt": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.throughput")));
                    break;
                }
            }
            Writer w = new BufferedWriter(f);
            w.write("Bounded throughput (k events/s):" + config.getDouble("bound", 0) + "\n");
            w.write("predict throughput (k events/s):" + config.getDouble("predict", 0) + "\n");
            w.write("finished measurement (k events/s):" + record.getPercentile(50) + "("
                    + (record.getPercentile(50) / config.getDouble("predict", 0)) + ")" + "\n");

            w.close();
            f.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
