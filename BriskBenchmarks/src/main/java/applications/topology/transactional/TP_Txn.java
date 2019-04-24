package applications.topology.transactional;


import applications.bolts.lr.txn.TP_nocc;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Conf;
import applications.constants.LinearRoadConstants.Field;
import applications.datatype.util.LRTopologyControl;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.LinearRoadConstants.PREFIX;
import static applications.constants.MicroBenchmarkConstants.Conf.Executor_Threads;
import static engine.content.Content.CCOption_LOCK;

public class TP_Txn extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TP_Txn.class);
    private final int accidentBoltThreads;
    private final int dailyExpBoltThreads;
    private final int toll_cv_BoltThreads, toll_las_BoltThreads, toll_pos_BoltThreads;


    private final int DispatcherBoltThreads;
    private final int COUNT_VEHICLES_Threads;
    private final int AccidentNotificationBoltThreads;
    private final int AccountBalanceBoltThreads;
    private final int averageSpeedThreads;
    private final int latestAverageVelocityThreads;

    public TP_Txn(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
        DispatcherBoltThreads = config.getInt(Conf.DispatcherBoltThreads, 1);
        COUNT_VEHICLES_Threads = config.getInt(Conf.COUNT_VEHICLES_Threads, 1);
        averageSpeedThreads = config.getInt(Conf.AverageSpeedThreads, 1);
        latestAverageVelocityThreads = config.getInt(Conf.LatestAverageVelocityThreads, 1);
        toll_cv_BoltThreads = config.getInt(Conf.toll_cv_BoltThreads, 1);
        toll_las_BoltThreads = config.getInt(Conf.toll_las_BoltThreads, 1);
        toll_pos_BoltThreads = config.getInt(Conf.toll_pos_BoltThreads, 1);
        accidentBoltThreads = config.getInt(Conf.AccidentDetectionBoltThreads, 1);
        AccidentNotificationBoltThreads = config.getInt(Conf.AccidentNotificationBoltThreads, 1);
        AccountBalanceBoltThreads = config.getInt(Conf.AccountBalanceBoltThreads, 1);
        dailyExpBoltThreads = config.getInt(Conf.dailyExpBoltThreads, 1);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {

        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, spoutThreads);

            switch (config.getInt("CCOption", 0)) {
                case CCOption_LOCK: {//no-order
                    builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TP_nocc(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(LinearRoadConstants.Component.SPOUT));
                    break;
                }
//
//                case CCOption_OrderLOCK: {//LOB
//
//                    builder.setBolt(MicroBenchmarkConstants.Component.EXECUTOR, new Bolt_olb(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(MicroBenchmarkConstants.Component.SPOUT));
//                    break;
//                }
//                case CCOption_LWM: {//LWM
//
//                    builder.setBolt(MicroBenchmarkConstants.Component.EXECUTOR, new Bolt_lwm(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(MicroBenchmarkConstants.Component.SPOUT));
//                    break;
//                }
//                case CCOption_TStream: {//T-Stream
//
//                    builder.setBolt(MicroBenchmarkConstants.Component.EXECUTOR, new Bolt_ts(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(MicroBenchmarkConstants.Component.SPOUT));
//                    break;
//                }
//                case CCOption_SStore: {//SStore
//
//                    builder.setBolt(MicroBenchmarkConstants.Component.EXECUTOR, new Bolt_sstore(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(MicroBenchmarkConstants.Component.SPOUT));
//                    break;
//                }

            }


//            builder.setBolt(LinearRoadConstants.Component.PARSER, new StringParserBolt(parser,
//                            new Fields(Field.TEXT))
//                    , config.getInt(Conf.PARSER_THREADS, 1)
//                    , new ShuffleGrouping(LRTopologyControl.SPOUT));

//            builder.setBolt(LRTopologyControl.DISPATCHER, new DispatcherBolt(), DispatcherBoltThreads,
//                    new ShuffleGrouping(LinearRoadConstants.Component.PARSER));
//
//            builder.setBolt(LRTopologyControl.AVERAGE_SPEED_BOLT, new AverageVehicleSpeedBolt(), averageSpeedThreads,
//                    new ShuffleGrouping(
//                            LRTopologyControl.DISPATCHER,
//                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
//                    )
//            );
////
//            builder.setBolt(LRTopologyControl.ACCIDENT_DETECTION_BOLT, new AccidentDetectionBolt(), accidentBoltThreads,
//                    new ShuffleGrouping(
//                            LRTopologyControl.DISPATCHER,
//                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
////							, new Fields(LRTopologyControl.XWAY_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME)
//                    )
//            );
//
//            builder.setBolt(LRTopologyControl.COUNT_VEHICLES_BOLT, new CountVehiclesBolt(), COUNT_VEHICLES_Threads,
////					new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX), COUNT_VEHICLES_Threads,
//                    new ShuffleGrouping(
//                            LRTopologyControl.DISPATCHER, LRTopologyControl.POSITION_REPORTS_STREAM_ID
////							, SegmentIdentifier.getSchema()
//                    )
//            );
//
//
//            builder.setBolt(LRTopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), latestAverageVelocityThreads,
//                    new ShuffleGrouping(
//                            LRTopologyControl.AVERAGE_SPEED_BOLT,
//                            LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID
//                    ));
//
//            builder.setBolt(LRTopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new AccidentNotificationBolt(), AccidentNotificationBoltThreads,
//                    new ShuffleGrouping(LRTopologyControl.DISPATCHER, //FieldsGrouping
//                            LRTopologyControl.POSITION_REPORTS_STREAM_ID// streamId
//                    )
//            );
//
//            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME, new TollNotificationBolt_pos(), toll_pos_BoltThreads
//                    , new ShuffleGrouping(LRTopologyControl.DISPATCHER, LRTopologyControl.POSITION_REPORTS_STREAM_ID
//                    )
//            );
//
//            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME, new TollNotificationBolt_cv(), toll_cv_BoltThreads
//                    , new ShuffleGrouping(LRTopologyControl.COUNT_VEHICLES_BOLT, LRTopologyControl.CAR_COUNTS_STREAM_ID)
//            );
//
//            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME, new TollNotificationBolt_las(), toll_las_BoltThreads
//                    , new ShuffleGrouping(LRTopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, LRTopologyControl.LAVS_STREAM_ID)
//            );
//
//            builder.setSink(LRTopologyControl.SINK, sink, sinkThreads
//                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME,
//                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
//                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME,
//                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
//                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME,
//                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
//            );

            builder.setSink(LinearRoadConstants.Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(LinearRoadConstants.Component.EXECUTOR)
            );

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        return builder.createTopology();
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
