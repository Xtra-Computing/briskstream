package applications.general.topology.classical;

import applications.general.bolts.comm.GeneralParserBolt;
import applications.general.bolts.lg.GeoStatsBolt;
import applications.general.bolts.lg.GeographyBolt;
import applications.general.bolts.lg.StatusCountBolt;
import applications.general.bolts.lg.VolumeCountBolt;
import applications.general.sink.BaseSink;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
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
    private final int volumeCountThreads;
    private final int statusCountThreads;
    private final int geoFinderThreads;
    private final int geoStatsThreads;
    int countSinkThreads;
    int statusSinkThreads;
    int countrySinkThreads;
    private BaseSink countSink;
    private BaseSink statusSink;
    private BaseSink countrySink;

    public LogProcessing(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
        countSinkThreads = config.getInt(getConfigKey("status"), 1);
        statusSinkThreads = config.getInt(getConfigKey("status"), 1);
        countrySinkThreads = config.getInt(getConfigKey("country"), 1);

        volumeCountThreads = config.getInt(Conf.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInt(Conf.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads = config.getInt(Conf.GEO_FINDER_THREADS, 1);
        geoStatsThreads = config.getInt(Conf.GEO_STATS_THREADS, 1);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
        countSink = loadSink("count");
        statusSink = loadSink("status");
        countrySink = loadSink("country");
    }

    @Override
    public Topology buildTopology() {

        spout.setFields(new Fields(Field.TEXT));

        try {
            builder.setSpout(Component.SPOUT, spout, spoutThreads);


            builder.setBolt(Component.PARSER, new GeneralParserBolt(parser,
                            new Fields(Field.IP, Field.TIMESTAMP, Field.TIMESTAMP_MINUTES,
                                    Field.REQUEST, Field.RESPONSE, Field.BYTE_SIZE))
                    , config.getInt(Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));


            builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads,
                    new ShuffleGrouping(Component.PARSER));


            builder.setBolt(Component.STATUS_COUNTER, new StatusCountBolt(), statusCountThreads,
                    new FieldsGrouping(Component.PARSER
                            , new Fields(Field.RESPONSE))
            );

            builder.setBolt(Component.VOLUME_COUNTER, new VolumeCountBolt(), volumeCountThreads,
                    new FieldsGrouping(Component.PARSER
                            , new Fields(Field.TIMESTAMP_MINUTES))
            );

            builder.setBolt(Component.GEO_STATS, new GeoStatsBolt(), geoStatsThreads,
                    new FieldsGrouping(Component.GEO_FINDER
                            , new Fields(Field.COUNTRY))
            );


//			builder.setBolt("STATUS_COUNTER_aggregator", new StatusCountBolt(), statusCountThreads,
//					new FieldsGrouping(Component.STATUS_COUNTER, new Fields(Field.RESPONSE)));
//
//			builder.setBolt("VOLUME_COUNTER_aggregator", new VolumeCountBolt(), statusCountThreads,
//					new FieldsGrouping(Component.VOLUME_COUNTER, new Fields(Field.TIMESTAMP_MINUTES)));
//
//			builder.setBolt("GEO_STATS_aggregator", new GeoStatsBolt(), statusCountThreads,
//					new FieldsGrouping(Component.GEO_STATS, new Fields(Field.COUNTRY)));


            builder.setBolt(Component.STATUS_SINK, statusSink, statusSinkThreads,
                    new ShuffleGrouping(Component.STATUS_COUNTER));

            builder.setBolt(Component.VOLUME_SINK, countSink, countSinkThreads,
                    new ShuffleGrouping(Component.VOLUME_COUNTER));
//
            builder.setBolt(Component.GEO_SINK, countrySink, countrySinkThreads,
                    new ShuffleGrouping(Component.GEO_STATS));
//
//
            //Use global sink instead.
            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.VOLUME_SINK)
                    , new ShuffleGrouping(Component.STATUS_SINK)
                    , new ShuffleGrouping(Component.GEO_SINK)
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
