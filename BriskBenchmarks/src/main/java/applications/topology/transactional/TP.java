package applications.topology.transactional;


import applications.bolts.comm.StringParserBolt;
import applications.bolts.lr.*;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Conf;
import applications.constants.LinearRoadConstants.Field;
import applications.datatype.util.LRTopologyControl;
import applications.datatype.util.SegmentIdentifier;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.AllGrouping;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.TPConstants.PREFIX;
import static applications.datatype.util.LRTopologyControl.*;

public class TP extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TP.class);
//    private final int toll_cv_BoltThreads, toll_las_BoltThreads, toll_pos_BoltThreads;

    private final int DispatcherBoltThreads;
    private final int COUNT_VEHICLES_Threads;
    private final int AccidentNotificationBoltThreads;
    private final int AccountBalanceBoltThreads;
    private final int averageVehicleSpeedThreads;
    private final int averageSpeedThreads;
    private final int latestAverageVelocityThreads;
    private final int toll_BoltThreads;

    public TP(String topologyName, Configuration config) {
        super(topologyName, config);
        DispatcherBoltThreads = config.getInt(Conf.DispatcherBoltThreads, 1);
        COUNT_VEHICLES_Threads = config.getInt(Conf.COUNT_VEHICLES_Threads, 1);
        averageVehicleSpeedThreads = config.getInt(Conf.AverageVehicleSpeedThreads, 1);
        averageSpeedThreads = config.getInt(Conf.AverageSpeedThreads, 1);
        latestAverageVelocityThreads = config.getInt(Conf.LatestAverageVelocityThreads, 1);
        toll_BoltThreads = config.getInt(Conf.tollBoltThreads, 1);
        AccidentNotificationBoltThreads = config.getInt(Conf.AccidentNotificationBoltThreads, 1);
        AccountBalanceBoltThreads = config.getInt(Conf.AccountBalanceBoltThreads, 1);
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
            builder.setSpout(LRTopologyControl.SPOUT, spout, 1);// single spout.

            //common part

            builder.setBolt(LinearRoadConstants.Component.PARSER
                    , new StringParserBolt(parser, new Fields(Field.TEXT)), config.getInt(Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(LRTopologyControl.SPOUT));

            builder.setBolt(LRTopologyControl.DISPATCHER,
                    new DispatcherBolt(), DispatcherBoltThreads,
                    new ShuffleGrouping(LinearRoadConstants.Component.PARSER));

            //accident query -- not in use. There's no accident.

            //speed query

            builder.setBolt(LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME,//calculate average vehicle speed -- Avgsv, window = 1 min, slide = 1 report.
                    new AverageVehicleSpeedBolt(), averageVehicleSpeedThreads, //ideally, we should keep input tuple to this operator sorted by TimestampMerger, PositionReport.TIME_IDX
                    new FieldsGrouping(
                            LRTopologyControl.DISPATCHER,
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
                            , SegmentIdentifier.getSchema()
                    )
            );

            builder.setBolt(LAST_AVERAGE_SPEED_BOLT_NAME, //calculate the 5-minute average road speed considering all vehicles.
                    new LatestAverageVelocityBolt(), latestAverageVelocityThreads,//window = 5 min, slide=1 report. //ideally, we should keep input tuple to this operator sorted by TimestampMerger, PositionReport.TIME_IDX
                    new FieldsGrouping(
                            LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME
                            , SegmentIdentifier.getSchema()
                    ));


            //count query

            builder.setBolt(COUNT_VEHICLES_BOLT, //calculate number of distinct vehicles on a road.
                    new CountVehiclesBolt(), COUNT_VEHICLES_Threads,
                    new FieldsGrouping(
                            LRTopologyControl.DISPATCHER,
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
                            , SegmentIdentifier.getSchema()
                    )
            );


            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                    new TollNotificationBolt()
//                    new TimestampMerger(new TollNotificationBolt(), new TollInputStreamsTsExtractor())
                    , toll_BoltThreads

                    , new FieldsGrouping(LRTopologyControl.DISPATCHER,//position report..
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID,
                            new Fields(LRTopologyControl.VEHICLE_ID_FIELD_NAME))

                    , new AllGrouping(LAST_AVERAGE_SPEED_BOLT_NAME, LAVS_STREAM_ID)//broadcast road speed information to TN.

                    , new AllGrouping(COUNT_VEHICLES_BOLT, CAR_COUNTS_STREAM_ID)//broadcast vehicle count information to TN.
            );

            builder.setSink(LRTopologyControl.SINK, sink, 1 // single sink.
                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TOLL_NOTIFICATIONS_STREAM_ID)
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
