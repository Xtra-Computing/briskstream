package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt;
import applications.bolts.sd.MovingAverageBolt;
import applications.bolts.sd.SpikeDetectionBolt;
import applications.topology.BasicTopology;
import constants.SpikeDetectionConstants;
import constants.SpikeDetectionConstants.Component;
import constants.SpikeDetectionConstants.Field;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import static constants.SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS;
import static constants.SpikeDetectionConstants.PREFIX;

public class SpikeDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);

    public SpikeDetection(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public StormTopology buildTopology() {

        spout.setFields(new Fields(Field.TEXT));
//        spout.setFields(Marker_STREAM_ID, new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.PARSER, new ParserBolt(parser
                        , new Fields(Field.DEVICE_ID, Field.TIMESTAMP, Field.VALUE)
                )
                , config.getInt(SpikeDetectionConstants.Conf.PARSER_THREADS, 1))
                .shuffleGrouping(Component.SPOUT)
//                .shuffleGrouping(Component.SPOUT, Marker_STREAM_ID);
        ;
        builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(),
                config.getInt(MOVING_AVERAGE_THREADS, 1))
                .fieldsGrouping(Component.PARSER, new Fields(Field.DEVICE_ID))
//                .fieldsGrouping(Component.PARSER, Marker_STREAM_ID, new Fields(Field.DEVICE_ID));
        ;
        builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(),
                config.getInt(SPIKE_DETECTOR_THREADS, 1))
                .shuffleGrouping(Component.MOVING_AVERAGE)
//                .shuffleGrouping(Component.MOVING_AVERAGE, Marker_STREAM_ID);
        ;
        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.SPIKE_DETECTOR)
//                .globalGrouping(Component.SPIKE_DETECTOR, Marker_STREAM_ID);
        ;
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
