package applications.general.topology.benchmarks;

import applications.general.bolts.comm.ParserBolt_latency;
import applications.general.bolts.lg.GeoStatsBolt_latency;
import applications.general.bolts.lg.GeographyBolt_latency;
import applications.general.bolts.lg.StatusCountBolt_latency;
import applications.general.bolts.lg.VolumeCountBolt_latency;
import applications.general.sink.BaseSink;
import applications.general.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static applications.constants.LogProcessingConstants.*;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 *
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessing_latency extends BasicTopology {
	private static final Logger LOG = LoggerFactory.getLogger(LogProcessing_latency.class);
	private BaseSink countSink;
	private BaseSink statusSink;
	private BaseSink countrySink;

	private int countSinkThreads;
	private int statusSinkThreads;
	private int countrySinkThreads;
	private int volumeCountThreads;
	private int statusCountThreads;
	private int geoFinderThreads;
	private int geoStatsThreads;
	private int batch;

	public LogProcessing_latency(String topologyName, Config config) {
		super(topologyName, config);
//        initilize_parser();
		countSinkThreads = (int) config.get(getConfigKey(Conf.SINK_THREADS, "status"));
		statusSinkThreads = (int) config.get(getConfigKey(Conf.SINK_THREADS, "status"));
		countrySinkThreads = (int) config.get(getConfigKey(Conf.SINK_THREADS, "country"));

		volumeCountThreads = (int) config.get(Conf.VOLUME_COUNTER_THREADS);
		statusCountThreads = (int) config.get(Conf.STATUS_COUNTER_THREADS);
		geoFinderThreads = (int) config.get(Conf.GEO_FINDER_THREADS);
		geoStatsThreads = (int) config.get(Conf.GEO_STATS_THREADS);
	}

	public void initialize() {
		super.initialize();
		sink = loadSink();
		countSink = loadSink("count");
		statusSink = loadSink("status");
		countrySink = loadSink("country");
	}

	@Override
	public FlinkTopology buildTopology() {
		batch = config.getInt("batch");

		spout.setFields(new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP));


		builder.setSpout(Component.SPOUT, spout, spoutThreads);
//            builder.setSpout(Component.SPOUT, spout, spoutThreads);

		builder.setBolt(Component.PARSER, new ParserBolt_latency(parser,
						new Fields(Field.IP, Field.TIMESTAMP, Field.TIMESTAMP_MINUTES,
								Field.REQUEST, Field.RESPONSE, Field.BYTE_SIZE, MSG_ID, SYSTEMTIMESTAMP)
				)
				, config.getInt(Conf.PARSER_THREADS, 1)).
				shuffleGrouping(Component.SPOUT);

//            builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads,
		builder.setBolt(Component.GEO_FINDER, new GeographyBolt_latency(), geoFinderThreads)
				.shuffleGrouping(Component.PARSER);

		builder.setBolt(Component.STATUS_COUNTER, new StatusCountBolt_latency(), statusCountThreads)
				.fieldsGrouping(Component.PARSER, new Fields(Field.RESPONSE));

		builder.setBolt(Component.VOLUME_COUNTER, new VolumeCountBolt_latency(), volumeCountThreads).
				fieldsGrouping(Component.PARSER, new Fields(Field.TIMESTAMP_MINUTES));

		builder.setBolt(Component.GEO_STATS, new GeoStatsBolt_latency(), geoStatsThreads).
				fieldsGrouping(Component.GEO_FINDER, new Fields(Field.COUNTRY));

		builder.setBolt(Component.VOLUME_SINK, countSink, sinkThreads).
				shuffleGrouping(Component.VOLUME_COUNTER);
//
		builder.setBolt(Component.STATUS_SINK, statusSink, sinkThreads).
				shuffleGrouping(Component.STATUS_COUNTER);

		builder.setBolt(Component.GEO_SINK, countrySink, sinkThreads).
				shuffleGrouping(Component.GEO_STATS);

		//Use global sink instead.
		builder.setBolt(Component.SINK, sink, sinkThreads).
//                shuffleGrouping(Component.VOLUME_SINK);
		shuffleGrouping(Component.STATUS_SINK);
//                shuffleGrouping(Component.GEO_SINK);

		return FlinkTopology.createTopology(builder, config);
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
