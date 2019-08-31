package applications.topology.benchmarks;


import applications.bolts.comm.ParserBolt;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Conf;
import applications.constants.LinearRoadConstants.Field;
import applications.datatypes.util.SegmentIdentifier;
import applications.datatypes.util.TopologyControl;
import applications.topology.BasicTopology;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static applications.constants.LinearRoadConstants.PREFIX;

/**
 * @author mayconbordin
 */
public class LinearRoad extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LinearRoad.class);
    private int accidentBoltThreads;
    private int dailyExpBoltThreads;
    private int tollBoltThreads;
    private int DispatcherBoltThreads;
    private int AverageSpeedThreads;
    private int CountThreads;
    private int LatestAverageVelocityThreads;
    private int AccidentNotificationBoltThreads;
    private int AccountBalanceBoltThreads;
    private int batch = 0;

    public LinearRoad(String topologyName, Config config) {
        super(topologyName, config);
//        initilize_parser();
        DispatcherBoltThreads = (int) config.get(Conf.DispatcherBoltThreads);
        AverageSpeedThreads = 1;//(int) config.get(Conf.AverageSpeedThreads);
        CountThreads = (int) config.get(Conf.CountThreads);
        LatestAverageVelocityThreads = 1;//(int) config.get(Conf.LatestAverageVelocityThreads);
        tollBoltThreads = (int) config.get(Conf.tollBoltThreads);
        accidentBoltThreads = (int) config.get(Conf.accidentBoltThreads);
        AccidentNotificationBoltThreads = (int) config.get(Conf.AccidentNotificationBoltThreads);
        AccountBalanceBoltThreads = (int) config.get(Conf.AccountBalanceBoltThreads);
        dailyExpBoltThreads = (int) config.get(Conf.dailyExpBoltThreads);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public StormTopology buildTopology() {
        // builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout
        batch = config.getInt("batch");

        List<String> fields = new LinkedList<String>(Arrays.asList(TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.DIRECTION_FIELD_NAME));


        spout.setFields(new Fields(Field.TEXT));//output of a spouts
        builder.setSpout(TopologyControl.SPOUT, spout, spoutThreads);

        builder.setBolt(LinearRoadConstants.Component.PARSER, new ParserBolt(parser,
                        new Fields(Field.TEXT))
                , config.getInt(Conf.PARSER_THREADS, 1)).
                shuffleGrouping(TopologyControl.SPOUT);
//
        builder.setBolt(TopologyControl.DISPATCHER, new DispatcherBolt(), DispatcherBoltThreads).
                shuffleGrouping(LinearRoadConstants.Component.PARSER);

//            builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT, new AverageVehicleSpeedBolt(), AverageSpeedThreads,
//                    new fieldsGrouping(
//                            TopologyControl.DISPATCHER,
//                            TopologyControl.POSITION_REPORTS_STREAM_ID,
//                            new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
//                                    TopologyControl.DIRECTION_FIELD_NAME)));
////
        builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT, new AccidentDetectionBolt(), accidentBoltThreads).
                fieldsGrouping(
                        TopologyControl.DISPATCHER,
                        TopologyControl.POSITION_REPORTS_STREAM_ID,
                        new Fields(TopologyControl.XWAY_FIELD_NAME,
                                TopologyControl.DIRECTION_FIELD_NAME));

        builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT,
//                    new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX), CountThreads,
                new CountVehiclesBolt(), CountThreads).
                fieldsGrouping(
                        TopologyControl.DISPATCHER, TopologyControl.POSITION_REPORTS_STREAM_ID,
                        SegmentIdentifier.getSchema());
        builder.setBolt(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME, new DailyExpenditureBolt(), dailyExpBoltThreads).
                shuffleGrouping(TopologyControl.DISPATCHER,
                        TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID);

//            builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), LatestAverageVelocityThreads,
//                    new fieldsGrouping(
//                            TopologyControl.AVERAGE_SPEED_BOLT,
//                            TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
//                            SegmentIdentifier.getSchema()));
////
        builder.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
                new AccidentNotificationBolt(), AccidentNotificationBoltThreads)
                .fieldsGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT,
                        TopologyControl.ACCIDENTS_STREAM_ID, // streamId
                        new Fields(TopologyControl.POS_REPORT_FIELD_NAME)).
                fieldsGrouping(TopologyControl.DISPATCHER,
                        TopologyControl.POSITION_REPORTS_STREAM_ID, // streamId
                        new Fields(TopologyControl.XWAY_FIELD_NAME,
                                TopologyControl.DIRECTION_FIELD_NAME));
//
        builder.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, new TollNotificationBolt(), tollBoltThreads).
                allGrouping(TopologyControl.COUNT_VEHICLES_BOLT, TopologyControl.CAR_COUNTS_STREAM_ID)
                .fieldsGrouping(
                        TopologyControl.ACCIDENT_DETECTION_BOLT,
                        TopologyControl.ACCIDENTS_STREAM_ID,
                        new Fields(
                                TopologyControl.POS_REPORT_FIELD_NAME,
                                TopologyControl.SEGMENT_FIELD_NAME))

                .fieldsGrouping(TopologyControl.DISPATCHER,
                        TopologyControl.POSITION_REPORTS_STREAM_ID,
                        new Fields(fields));

        builder.setBolt(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME
                , new AccountBalanceBolt(), AccountBalanceBoltThreads

        ).fieldsGrouping(TopologyControl.DISPATCHER,
                TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
//            );


                .fieldsGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,//same vid go to same account balance.
                        TopologyControl.TOLL_ASSESSMENTS_STREAM_ID,
                        new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME));

        builder.setBolt(TopologyControl.SINK, sink, sinkThreads)
//                    new shuffleGrouping(TopologyControl.SPOUT));

                .shuffleGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                        TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)

                .shuffleGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
                        TopologyControl.ACCIDENTS_NOIT_STREAM_ID)

//            );
                .
//
        shuffleGrouping(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME,
        TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID)
//
                .shuffleGrouping(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME,
                        TopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID);

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
