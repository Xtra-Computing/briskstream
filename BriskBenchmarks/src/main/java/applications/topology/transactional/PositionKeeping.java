package applications.topology.transactional;


import applications.bolts.pk.*;
import applications.constants.PositionKeepingConstants.Component;
import applications.constants.PositionKeepingConstants.Field;
import applications.topology.transactional.initializer.PKInitializer;
import applications.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.TransactionTopology;
import engine.common.PartitionedOrderLock;
import engine.common.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static applications.constants.PositionKeepingConstants.Conf.PK_THREADS;
import static applications.constants.PositionKeepingConstants.Constant.NUM_MACHINES;
import static applications.constants.PositionKeepingConstants.PREFIX;
import static utils.PartitionHelper.getPartition_interval;
import static utils.PartitionHelper.setPartition_interval;

/**
 * Based on Spike Detection.
 */

public class PositionKeeping extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(PositionKeeping.class);

    public PositionKeeping(String topologyName, Configuration config) {
        super(topologyName, config);

    }

    public static String getPrefix() {
        return PREFIX;
    }

    static int GenerateInteger(final int min, final int max) {
        Random r = new Random();
        return r.nextInt(max) + min;
    }

    public TableInitilizer initializeDB(SpinLock[] spinlock) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");

        TableInitilizer ini = new PKInitializer(db, scale_factor, theta, tthread, config);
        ini.creates_Table();

        setPartition_interval((int) (Math.ceil(NUM_MACHINES / (double) tthread)), tthread);

        SpinLock[] spinlock_ = null;
        if (config.getBoolean("partition", false)) {
            spinlock_ = new SpinLock[tthread];//number of threads -- number of cores -- number of partitions.

            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();

            ini.loadData(scale_factor, theta, getPartition_interval(), spinlock_);

            //initialize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        } else {
            ini.loadData(scale_factor, theta);
        }
        return null;
    }

    @Override
    public Topology buildTopology() {
        try {

            spout.setFields(new Fields(Field.DEVICE_ID));//output of a spouts
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            switch (config.getInt("CCOption", 0)) {

                case 0: {//no-order
                    builder.setBolt(Component.PK, new PKBolt_nocc(0)//
                            , config.getInt(PK_THREADS, 2)
//                            , new FieldsGrouping(Component.SPOUT, new Fields(Field.DEVICE_ID)));
                            , new ShuffleGrouping(Component.SPOUT));
                    break;
                }

                case 1: {//LOB
                    builder.setBolt(Component.PK, new PKBolt_olb(0)//
                            , config.getInt(PK_THREADS, 2)
                            , new ShuffleGrouping(Component.SPOUT));
                    break;
                }

                case 2: {//LWM
                    builder.setBolt(Component.PK, new PKBolt_lwm(0)//
                            , config.getInt(PK_THREADS, 2)
                            , new ShuffleGrouping(Component.SPOUT));
                    break;
                }

                case 3: {//T-Stream
                    builder.setBolt(Component.PK, new PKBolt_ts(0)//
                            , config.getInt(PK_THREADS, 2)
                            , new ShuffleGrouping(Component.SPOUT));
                    break;
                }

                case 4: {//SStore
                    builder.setBolt(Component.PK, new PKBolt_sstore(0)//
                            , config.getInt(PK_THREADS, 2)
//                            , new FieldsGrouping(Component.SPOUT, new Fields(Field.DEVICE_ID)));
                            , new ShuffleGrouping(Component.SPOUT));
                    break;
                }
            }

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.PK)
            );

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
