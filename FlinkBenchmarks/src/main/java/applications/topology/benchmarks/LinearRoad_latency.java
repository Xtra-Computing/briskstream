package applications.topology.benchmarks;


import applications.bolts.comm.ParserBolt_latency;
import applications.bolts.lr.*;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Conf;
import applications.datatypes.util.SegmentIdentifier;
import applications.datatypes.util.TopologyControl;
import applications.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.*;
import static applications.constants.LinearRoadConstants.PREFIX;

/**
 * @author mayconbordin
 */
public class LinearRoad_latency extends BasicTopology {
	private static final Logger LOG = LoggerFactory.getLogger(LinearRoad_latency.class);
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

	public LinearRoad_latency(String topologyName, Config config) {
		super(topologyName, config);
//        initilize_parser();
		DispatcherBoltThreads = 1;
		AverageSpeedThreads = 1;//(int) config.get(Conf.AverageSpeedThreads);
		CountThreads = 1;
		LatestAverageVelocityThreads = 1;//(int) config.get(Conf.LatestAverageVelocityThreads);
		tollBoltThreads = 1;
		accidentBoltThreads = 1;
		AccidentNotificationBoltThreads = 1;
		AccountBalanceBoltThreads = 1;
		dailyExpBoltThreads = 1;
	}

	public void initialize() {
		super.initialize();
		sink = loadSink();
	}

	@Override
	public FlinkTopology buildTopology() {
		// builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout
		batch = config.getInt("batch");


		spout.setFields(new Fields(TEXT, MSG_ID, SYSTEMTIMESTAMP));//output of a spouts
		builder.setSpout(TopologyControl.SPOUT, spout, spoutThreads);

		builder.setBolt(LinearRoadConstants.Component.PARSER
				, new ParserBolt_latency(parser,
						new Fields(TEXT, MSG_ID, SYSTEMTIMESTAMP)
				)
				, config.getInt(Conf.PARSER_THREADS, 1)).
				shuffleGrouping(TopologyControl.SPOUT);
//
		builder.setBolt(TopologyControl.DISPATCHER, new DispatcherBolt_latency(), DispatcherBoltThreads).
				shuffleGrouping(LinearRoadConstants.Component.PARSER);


		builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT, new AccidentDetectionBolt_latency(), accidentBoltThreads).
				fieldsGrouping(
						TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID,
						new Fields(TopologyControl.XWAY_FIELD_NAME,
								TopologyControl.DIRECTION_FIELD_NAME));

		builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT,
//                    new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX), CountThreads,
				new CountVehiclesBolt_latency(), CountThreads).
				fieldsGrouping(
						TopologyControl.DISPATCHER, TopologyControl.POSITION_REPORTS_STREAM_ID,
						SegmentIdentifier.getSchema());

		builder.setBolt(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME, new DailyExpenditureBolt_latency(), dailyExpBoltThreads).
				shuffleGrouping(TopologyControl.DISPATCHER,
						TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID);

//            builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), LatestAverageVelocityThreads,
//                    new fieldsGrouping(
//                            TopologyControl.AVERAGE_SPEED_BOLT,
//                            TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
//                            SegmentIdentifier.getSchema()));
////
		builder.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
				new AccidentNotificationBolt_latency(), AccidentNotificationBoltThreads)
				.fieldsGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT,
						TopologyControl.ACCIDENTS_STREAM_ID, // streamId
						new Fields(TopologyControl.POS_REPORT_FIELD_NAME
								, TopologyControl.SEGMENT_FIELD_NAME)).

				fieldsGrouping(TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID, // streamId
						new Fields(TopologyControl.XWAY_FIELD_NAME,
								TopologyControl.DIRECTION_FIELD_NAME));
//
		builder.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, new TollNotificationBolt_latency(), tollBoltThreads)
				.allGrouping(TopologyControl.COUNT_VEHICLES_BOLT, TopologyControl.CAR_COUNTS_STREAM_ID)
				.fieldsGrouping(
						TopologyControl.ACCIDENT_DETECTION_BOLT,
						TopologyControl.ACCIDENTS_STREAM_ID,
						new Fields(
								TopologyControl.POS_REPORT_FIELD_NAME,
								TopologyControl.SEGMENT_FIELD_NAME))

				.fieldsGrouping(TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID,
						new Fields(TopologyControl.XWAY_FIELD_NAME,
								TopologyControl.DIRECTION_FIELD_NAME));

		builder.setBolt(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME
				, new AccountBalanceBolt_latency(), AccountBalanceBoltThreads

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

//
				.shuffleGrouping(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME,
						TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID)
//
				.shuffleGrouping(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME,
						TopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID)
		;

		FlinkTopology topology = FlinkTopology.createTopology(builder, config);

		return topology;
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
