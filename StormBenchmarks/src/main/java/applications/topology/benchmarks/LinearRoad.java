package applications.topology.benchmarks;


import applications.bolts.comm.ParserBolt;
import applications.bolts.lr.*;
import applications.constants.LinearRoadConstants;
import applications.constants.LinearRoadConstants.Conf;
import applications.constants.LinearRoadConstants.Field;
import applications.data.util.TopologyControl;
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
	private final int accidentBoltThreads;
//	private final int dailyExpBoltThreads;
	private final int toll_cv_BoltThreads, toll_las_BoltThreads, toll_pos_BoltThreads;
	private final int DispatcherBoltThreads;
	private final int COUNT_VEHICLES_Threads;
	private final int AccidentNotificationBoltThreads;
//	private final int AccountBalanceBoltThreads;
	private final int averageSpeedThreads;
	private final int latestAverageVelocityThreads;

	public LinearRoad(String topologyName, Config config) {
		super(topologyName, config);
		DispatcherBoltThreads = (int) config.get(Conf.DispatcherBoltThreads);
		COUNT_VEHICLES_Threads = (int) config.get(Conf.COUNT_VEHICLES_Threads);
		averageSpeedThreads = (int) config.get(Conf.AverageSpeedThreads);
		latestAverageVelocityThreads = (int) config.get(Conf.LatestAverageVelocityThreads);
		toll_cv_BoltThreads = (int) config.get(Conf.toll_cv_BoltThreads);
		toll_las_BoltThreads = (int) config.get(Conf.toll_las_BoltThreads);
		toll_pos_BoltThreads = (int) config.get(Conf.toll_pos_BoltThreads);
		accidentBoltThreads = (int) config.get(Conf.AccidentDetectionBoltThreads);
		AccidentNotificationBoltThreads = (int) config.get(Conf.AccidentNotificationBoltThreads);
	}

	public void initialize() {
		super.initialize();
		sink = loadSink();
	}

	@Override
	public StormTopology buildTopology() {


		List<String> fields = new LinkedList<>(Arrays.asList(TopologyControl.XWAY_FIELD_NAME,
				TopologyControl.DIRECTION_FIELD_NAME));

		spout.setFields(new Fields(Field.TEXT));//output of a spouts
		builder.setSpout(TopologyControl.SPOUT, spout, spoutThreads);


		builder.setBolt(LinearRoadConstants.Component.PARSER
				, new ParserBolt(parser
						, new Fields(Field.TEXT)
				)
				, config.getInt(Conf.PARSER_THREADS, 1)).
				shuffleGrouping(TopologyControl.SPOUT);

		builder.setBolt(TopologyControl.DISPATCHER, new DispatcherBolt(), DispatcherBoltThreads).
				shuffleGrouping(LinearRoadConstants.Component.PARSER);

		builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT, new AverageVehicleSpeedBolt(), averageSpeedThreads).
				shuffleGrouping(
						TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID
				);

		builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT, new AccidentDetectionBolt(), accidentBoltThreads).
				shuffleGrouping(
						TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID);


		builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT, new CountVehiclesBolt(), COUNT_VEHICLES_Threads).
				shuffleGrouping(
						TopologyControl.DISPATCHER, TopologyControl.POSITION_REPORTS_STREAM_ID);

		builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), latestAverageVelocityThreads)
				.shuffleGrouping(
						TopologyControl.AVERAGE_SPEED_BOLT,
						TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID);

		builder.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new AccidentNotificationBolt(), AccidentNotificationBoltThreads)
				.shuffleGrouping(TopologyControl.DISPATCHER,
						TopologyControl.POSITION_REPORTS_STREAM_ID // streamId
				);

		builder.setBolt(TopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME, new TollNotificationBolt_pos(), toll_pos_BoltThreads).
				shuffleGrouping(TopologyControl.DISPATCHER, TopologyControl.POSITION_REPORTS_STREAM_ID);

		builder.setBolt(TopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME, new TollNotificationBolt_cv(), toll_pos_BoltThreads).
				shuffleGrouping(TopologyControl.COUNT_VEHICLES_BOLT, TopologyControl.CAR_COUNTS_STREAM_ID);

		builder.setBolt(TopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME, new TollNotificationBolt_las(), toll_pos_BoltThreads).
				shuffleGrouping(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID);


		builder.setBolt(TopologyControl.SINK, sink, sinkThreads)

				.shuffleGrouping(TopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME,
						TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)

				.shuffleGrouping(TopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME,
						TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)

				.shuffleGrouping(TopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME,
						TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID);

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
