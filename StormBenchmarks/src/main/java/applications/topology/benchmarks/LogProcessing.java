package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt;
import applications.bolts.lg.GeoStatsBolt;
import applications.bolts.lg.GeographyBolt;
import applications.bolts.lg.StatusCountBolt;
import applications.bolts.lg.VolumeCountBolt;
import applications.sink.BaseSink;
import applications.topology.BasicTopology;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.LogProcessingConstants.*;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 *
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessing extends BasicTopology {
	private static final Logger LOG = LoggerFactory.getLogger(LogProcessing.class);
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

	public LogProcessing(String topologyName, Config config) {
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
	public StormTopology buildTopology() {
		batch = config.getInt("batch");

		spout.setFields(new Fields(Field.TEXT));


		builder.setSpout(Component.SPOUT, spout, spoutThreads);
//            builder.setSpout(Component.SPOUT, spout, spoutThreads);

		builder.setBolt(Component.PARSER, new ParserBolt(parser,
						new Fields(Field.IP, Field.TIMESTAMP, Field.TIMESTAMP_MINUTES,
								Field.REQUEST, Field.RESPONSE, Field.BYTE_SIZE)
				)
				, config.getInt(Conf.PARSER_THREADS, 1)).
				shuffleGrouping(Component.SPOUT);

//            builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads,
		builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads).shuffleGrouping(Component.PARSER);

		builder.setBolt(Component.STATUS_COUNTER, new StatusCountBolt(), statusCountThreads)
				.fieldsGrouping(Component.PARSER, new Fields(Field.RESPONSE));

		builder.setBolt(Component.VOLUME_COUNTER, new VolumeCountBolt(), volumeCountThreads).
				fieldsGrouping(Component.PARSER, new Fields(Field.TIMESTAMP_MINUTES));

		builder.setBolt(Component.GEO_STATS, new GeoStatsBolt(), geoStatsThreads).
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
				shuffleGrouping(Component.VOLUME_SINK).
				shuffleGrouping(Component.STATUS_SINK).
				shuffleGrouping(Component.GEO_SINK);

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
