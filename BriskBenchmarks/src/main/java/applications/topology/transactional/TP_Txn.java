package applications.topology.transactional;


import applications.bolts.lr.DispatcherBolt;
import applications.bolts.tp.TPBolt_olb;
import applications.bolts.tp.TPBolt_ts;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Field;
import applications.datatype.util.LRTopologyControl;
import applications.datatype.util.SegmentIdentifier;
import applications.topology.transactional.initializer.TPInitializer;
import applications.topology.transactional.initializer.TableInitilizer;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.TransactionTopology;
import engine.common.PartitionedOrderLock;
import engine.common.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.enable_app_combo;
import static applications.constants.LinearRoadConstants.Conf.Executor_Threads;
import static applications.constants.TP_TxnConstants.Component.EXECUTOR;
import static applications.constants.TP_TxnConstants.Constant.NUM_SEGMENTS;
import static applications.constants.TP_TxnConstants.PREFIX;
import static engine.content.Content.CCOption_OrderLOCK;
import static engine.content.Content.CCOption_TStream;
import static utils.PartitionHelper.setPartition_interval;

public class TP_Txn extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TP_Txn.class);

    public TP_Txn(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }


    //TODO: Clean this method..
    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(NUM_SEGMENTS / (double) tthread)), tthread);
        TableInitilizer ini = new TPInitializer(db, scale_factor, theta, tthread, config);

        ini.creates_Table(config);


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


//        ini.loadData_Central(scale_factor, theta); // load data by multiple threads.

//        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

        return ini;
    }

    @Override
    public Topology buildTopology() {

        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, spoutThreads);

            if (enable_app_combo) {
                //spout only.

            } else {
                builder.setBolt(LRTopologyControl.DISPATCHER,
                        new DispatcherBolt(), 1,
                        new ShuffleGrouping(LRTopologyControl.SPOUT));

                switch (config.getInt("CCOption", 0)) {
//                case CCOption_LOCK: {//no-order
//                    builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TP_nocc(0)//not available
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(LinearRoadConstants.Component.SPOUT));
//                    break;
//                }

                    case CCOption_OrderLOCK: {//LOB

                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_olb(0)//
                                , config.getInt(Executor_Threads, 2),
                                new FieldsGrouping(
                                        LRTopologyControl.DISPATCHER,
                                        LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                        , SegmentIdentifier.getSchema())
                        );
                        break;
                    }
//                case CCOption_LWM: {//LWM
//
//                    builder.setBolt(GrepSumConstants.Component.EXECUTOR, new GSBolt_lwm(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(GrepSumConstants.Component.SPOUT));
//                    break;
//                }
                    case CCOption_TStream: {//T-Stream
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR,
                                new TPBolt_ts(0)//
                                , config.getInt(Executor_Threads, 2),
                                new FieldsGrouping(
                                        LRTopologyControl.DISPATCHER,
                                        LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                        , SegmentIdentifier.getSchema())
                        );
                        break;
                    }
//                case CCOption_SStore: {//SStore
//
//                    builder.setBolt(GrepSumConstants.Component.EXECUTOR, new GSBolt_sstore(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(GrepSumConstants.Component.SPOUT));
//                    break;
//                }

                }

                builder.setSink(LinearRoadConstants.Component.SINK, sink, sinkThreads
                        , new ShuffleGrouping(EXECUTOR)
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
