package applications.topology.transactional;


import applications.bolts.transactional.gs.*;
import applications.constants.GrepSumConstants.Component;
import applications.topology.transactional.initializer.MBInitializer;
import applications.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.topology.TransactionTopology;
import engine.common.PartitionedOrderLock;
import engine.common.SpinLock;
import engine.profiler.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static applications.CONTROL.enable_app_combo;
import static applications.constants.GrepSumConstants.Conf.Executor_Threads;
import static applications.constants.GrepSumConstants.PREFIX;
import static engine.content.Content.*;
import static utils.PartitionHelper.setPartition_interval;


public class GrepSum extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(GrepSum.class);

    public GrepSum(String topologyName, Configuration config) {
        super(topologyName, config);

    }

    public static String getPrefix() {
        return PREFIX;
    }

//    boolean read = true;
//    boolean write = true;

    static int GenerateInteger(final int min, final int max) {
        Random r = new Random();
        return r.nextInt(max) + min;
    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(Metrics.NUM_ITEMS / (double) tthread)), tthread);

        TableInitilizer ini = new MBInitializer(db, scale_factor, theta, tthread, config);

        ini.creates_Table(config);

        int num_partitions;


        if (config.getBoolean("partition", false)) {

            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();

//            ini.loadDB(scale_factor, theta, getPartition_interval(), spinlock_);

            //initilize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        } else {
//            ini.loadDB(scale_factor, theta);
        }
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {

            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            if (enable_app_combo) {
            //spout only.

            } else {

                switch (config.getInt("CCOption", 0)) {
                    case CCOption_LOCK: {//no-order

                        builder.setBolt(Component.EXECUTOR, new GSBolt_nocc(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }

                    case CCOption_OrderLOCK: {//LOB

                        builder.setBolt(Component.EXECUTOR, new GSBolt_olb(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_LWM: {//LWM

                        builder.setBolt(Component.EXECUTOR, new GSBolt_lwm(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_TStream: {//T-Stream

                        builder.setBolt(Component.EXECUTOR, new GSBolt_ts(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_SStore: {//SStore

                        builder.setBolt(Component.EXECUTOR, new GSBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_OTS: {//Timestamp Ordering

                        builder.setBolt(Component.EXECUTOR, new GSBolt_ots(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }

                }

                builder.setSink(Component.SINK, sink, sinkThreads
                        , new ShuffleGrouping(Component.EXECUTOR)
                );
            }
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
    }


    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
