package applications;

import applications.constants.*;
import applications.general.topology.*;
import applications.general.topology.faulttolerance.WordCount_FT;
import applications.general.topology.latency.LinearRoad_latency;
import applications.general.topology.latency.WordCount_latency;
import applications.general.topology.transactional.*;
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
import engine.common.SpinLock;
import engine.profiler.Metrics;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SINK_CONTROL;

import java.io.*;
import java.util.Properties;

import static applications.CONTROL.enable_app_combo;
import static applications.CONTROL.enable_profile;
import static applications.Constants.System_Plan_Path;
import static applications.constants.LinearRoadConstants.Conf.Executor_Threads;
import static applications.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static applications.constants.PositionKeepingConstants.Conf.PK_THREADS;
import static applications.constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import static applications.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static engine.content.Content.*;
import static engine.content.LWMContentImpl.LWM_CONTENT;
import static engine.content.LockContentImpl.LOCK_CONTENT;
import static engine.content.SStoreContentImpl.SSTORE_CONTENT;
import static engine.content.T_StreamContentImpl.T_STREAMCONTENT;
import static engine.content.common.ContentCommon.content_type;

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


        //Fault tolerance application
        driver.addApp("WordCount_FT", WordCount_FT.class);//

        //Transactional Application
        driver.addApp("GrepSum", GrepSum.class);//GS
        driver.addApp("StreamLedger", StreamLedger.class);//SL
        driver.addApp("OnlineBiding", OnlineBiding.class);//OB
        driver.addApp("TP_Txn", TP_Txn.class);//TP


        //special
        driver.addApp("TP", TP.class);//TP w/o shared states.

        //unfinished.
        driver.addApp("LeaderBoard", LeaderBoard.class);//
        driver.addApp("PositionKeeping", PositionKeeping.class);


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
        sinkThread.join((long) (30 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins

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

            if (enable_app_combo) {
                return SINK_CONTROL.getInstance().throughput;
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
    }

    private void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default
        // configuration
        // Prepared default configuration
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
                if (spout_class.equals("applications.general.spout.LocalStateSpout")) {
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
                default:
                    this.p = new applications.HUAWEI_Machine();
            }

            if (simulation) {
                LOG.info("Simulation: use machine:" + config.getInt("machine")
                        + " with sockets:" + config.getInt("num_socket")
                        + " and cores:" + config.getInt("num_cpu"));
//				config.put("num_socket", this.p.num_socket);
//				config.put("num_cpu", this.p.num_cores / this.p.num_socket);
            }

            //configure database.

//            int _combo_bid_size = 1;

            switch (config.getInt("CCOption", 0)) {
                case CCOption_LOCK://lock_ratio
                case CCOption_OrderLOCK://Ordered lock_ratio
                    content_type = LOCK_CONTENT;
                    break;
                case CCOption_LWM://LWM
                    content_type = LWM_CONTENT;
                    break;
                case CCOption_TStream:
                    content_type = T_STREAMCONTENT;//records the multi-version of table record.
                    break;
                case CCOption_SStore://SStore
                    content_type = SSTORE_CONTENT;//records the multi-version of table record.
                    break;
            }

            int max_hz = 0;
            boolean profile = config.getBoolean("profile");
            //  boolean benchmark = config.getBoolean("benchmark");
            //configure threads.
            int tthread = config.getInt("tthread");


            if (enable_app_combo) {
                config.put(BaseConstants.BaseConf.SPOUT_THREADS, tthread);


//                switch (config.getInt("CCOption", 0)) {
//
//                    case CCOption_OrderLOCK://Ordered lock_ratio
//                    case CCOption_LWM://LWM
//                    case CCOption_SStore://SStore
//                        _combo_bid_size = 1;
//                        break;
//                    default:
//                        _combo_bid_size = combo_bid_size;
//                }
//                SOURCE_CONTROL.getInstance().config(tthread, _combo_bid_size);

            } else
                config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);


            config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);
            config.put(BaseConstants.BaseConf.PARSER_THREADS, pthread);
            //set overhead_total parallelism, equally parallelism
            switch (application) {
                case "GrepSum": {
                    config.put("app", 0);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(GrepSumConstants.Conf.Executor_Threads, threads);
//                    double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
//                    int r_threads = (int) (threads * ratio_of_read);
//                    int w_threads = threads - r_threads;
//
//                    config.put(GrepSumConstants.Conf.SELECTOR_THREADS, r_threads);
//                    config.put(GrepSumConstants.Conf.INSERTOR_THREADS, w_threads);
                    break;
                }
                case "StreamLedger": {
                    config.put("app", 1);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(SL_THREADS, threads);
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
                case "TP_Txn": {
                    config.put("app", 3);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(Executor_Threads, threads);
                    break;
                }
                case "TP": {
                    config.put("app", 3);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(DEG_THREADS, threads);
//                    config.put(TEG_THREADS, threads);
//                    config.put(DT_THREADS, threads);
//                    config.put(TT_THREADS, threads);
                    config.put(Executor_Threads, threads);
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
                        int threads = (int) Math.floor(tthread / 2.0);
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

        // Run the topology
        double rt = runTopologyLocally(topology, config);

        if (CONTROL.enable_shared_state) {
            SpinLock[] spinlock = final_topology.spinlock;
            for (SpinLock lock : spinlock) {
                if (lock != null)
                    LOG.info("Partition" + lock + " being locked:\t" + lock.count + "\t times");
            }
        }


        Metrics metrics = Metrics.getInstance();

        if (rt != -1) {//returns normally.
            record.addValue(rt);
        }
        LOG.info("Bounded throughput (k events/s):" + config.getDouble("bound", 0));
        LOG.info("predict throughput (k events/s):" + config.getDouble("predict", 0));
        LOG.info("finished measurement (k events/s):\t" + record.getPercentile(50) + "\t( "
                + (Math.abs(record.getPercentile(50) - config.getDouble("predict", 0)) / config.getDouble("predict", 0)) + " )");

        if (enable_profile) {
            double overhead = 0;
            double stream_processing = 0;
            double txn_processing = 0;


            double useful_time = 0;
            double abort_time = 0;
            double ts_alloc_time = 0;
            double index_time = 0;
            double wait_time = 0;
            double lock_time = 0;
            double compute_time = 0;
//            double sum = 0;

            for (int i = 0; i < tthread; i++) {

                useful_time += metrics.useful_ratio[i].getMean();
//                abort_time += metrics.abort_ratio[i].getPercentile(50);
//                ts_alloc_time += metrics.ts_allocation[i].getPercentile(50);
                index_time += metrics.index_ratio[i].getMean();
                wait_time += metrics.sync_ratio[i].getMean();


                if (config.getInt("CCOption", 0) != CCOption_TStream)
                    lock_time += metrics.lock_ratio[i].getMean();

//                compute_time += metrics.exe_ratio[i].getMean();
//                sum += metrics.useful_ratio[i].getN();
                stream_processing += metrics.stream_total[i].getMean();
                overhead += metrics.overhead_total[i].getMean();
                txn_processing += metrics.txn_total[i].getMean();

                LOG.info(metrics.stream_total[i].toString());
            }


            //get average ratio per thread.

            useful_time = useful_time / tthread;
            abort_time = abort_time / tthread;
            ts_alloc_time = ts_alloc_time / tthread;
            index_time = index_time / tthread;
            wait_time = wait_time / tthread;
            lock_time = lock_time / tthread;
            compute_time = compute_time / tthread;

            stream_processing = stream_processing / tthread;
            overhead = overhead / tthread;
            txn_processing = txn_processing / tthread;


            //used in TSTREAM.

            String directory = System_Plan_Path
                    + OsUtils.OS_wrapper("BriskStream")
                    + OsUtils.OS_wrapper(topology.getPrefix())
                    + OsUtils.OS_wrapper("CCOption=" + String.valueOf(config.getInt("CCOption", 0)));


            File file = new File(directory);
            if (!file.mkdirs()) {
            }

            FileWriter f = null;
            StringBuilder sb = new StringBuilder();

            try {
                f = new FileWriter(new File(directory + OsUtils.OS_wrapper("breakdown(" + String.valueOf(checkpoint)) + ").txt"), true);
                Writer w = new BufferedWriter(f);

                w.write(String.valueOf(tthread));
                w.write(",");
                w.write(String.format("%.2f", overhead));//overhead per event
                w.write(",");
                w.write(String.format("%.2f", stream_processing));//average stream processing.
                w.write(",");
                w.write(String.format("%.2f", txn_processing * (useful_time)));//average txn processing * useful = state access.
                w.write(",");
                w.write(String.format("%.2f", txn_processing * (1 - (useful_time))));//state access overhead.
                w.write(",");
                w.write(String.format("%.2f", txn_processing));//average txn processing time.
                w.write(",");
                w.write(String.format("%.2f", useful_time));//useful ratio.
                w.write(",");
                w.write(String.format("%.2f", abort_time));//abort ratio.
                w.write(",");
                w.write(String.format("%.2f", wait_time));//sync ratio.
                w.write(",");
                w.write(String.format("%.2f", lock_time));//lock ratio.
                w.write(",");
                w.write(String.format("%.2f", 1 - (useful_time + abort_time + wait_time + lock_time)));//others ratio.
                w.write(",");
                w.write(String.format("%.2f", rt));//throughput
                w.write("\n");
                w.close();
                f.close();


                if (config.getInt("CCOption", 0) == CCOption_TStream) {//extra info

                    f = new FileWriter(new File(directory
                            + OsUtils.OS_wrapper("details(" + String.valueOf(tthread) + "," + String.valueOf(checkpoint)) + ").txt"), true);
                    w = new BufferedWriter(f);

                    for (int i = 0; i < tthread; i++) {
                        sb.append(String.valueOf(i));//which thread.
                        sb.append(",");
                        sb.append(String.format("%d", metrics.useful_ratio[i].getN()));//number of txns processed by the thread.
                        sb.append(",");
                        sb.append(String.format("%.2f", metrics.average_txn_construct[i].getPercentile(50)));//average construction time.
                        sb.append(",");
                        sb.append(String.format("%.2f", metrics.average_tp_submit[i].getPercentile(50)));//average submit time.
                        sb.append(",");
                        sb.append(String.format("%.2f", metrics.average_tp_w_syn[i].getPercentile(50) - metrics.average_tp_core[i].getPercentile(50)));//average sync time.
                        sb.append(",");
                        sb.append(String.format("%.2f", metrics.average_tp_core[i].getPercentile(50)));//average core tp time.
                        sb.append("\n");
                    }

                    w.write(sb.toString());
                    w.close();
                    f.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            LOG.info("===OVERALL===");

            LOG.info("Overhead on one input_event:" + String.format("%.2f", overhead));
            LOG.info("Stream Processing on one input_event:" + String.format("%.2f", stream_processing));
            LOG.info("TXN Processing on one input_event:" + String.format("%.2f", txn_processing));

            LOG.info("===BREAKDOWN TXN===");
            LOG.info("Useful time:\t" + String.format("%.2f", useful_time));
//            LOG.info("Abort time:\t" + String.format("%.2f", abort_time));
//            LOG.info("Ts_alloc. time:\t" + String.format("%.2f", ts_alloc_time ));
            LOG.info("Index_time time:\t" + String.format("%.2f", index_time));
            LOG.info("Wait_time time:\t" + String.format("%.2f", wait_time));
            LOG.info("lock_ratio time:\t" + String.format("%.2f", lock_time));

            LOG.info("====Details ====");
            LOG.info("\n" + sb.toString());
        }
/*
used only in Briskstream.
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

        String directory = System_Plan_Path
                + OsUtils.OS_wrapper("BriskStream")
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
*/

    }

}
