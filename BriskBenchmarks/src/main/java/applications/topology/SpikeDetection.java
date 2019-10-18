package applications.topology;

import applications.bolts.comm.SensorParserBolt;
import applications.bolts.sd.MovingAverageBolt;
import applications.bolts.sd.SpikeDetectionBolt;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import constants.SpikeDetectionConstants;
import constants.SpikeDetectionConstants.Component;
import constants.SpikeDetectionConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import static constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import static constants.SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS;
import static constants.SpikeDetectionConstants.PREFIX;

public class SpikeDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);

    public SpikeDetection(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.PARSER, new SensorParserBolt(parser, new Fields(Field.DEVICE_ID, Field.VALUE))
                    , config.getInt(SpikeDetectionConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(),
                    config.getInt(MOVING_AVERAGE_THREADS, 1)
                    , new FieldsGrouping(Component.PARSER, new Fields(Field.DEVICE_ID)
                    ));

            builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(),
                    config.getInt(SPIKE_DETECTOR_THREADS, 1)
                    , new ShuffleGrouping(Component.MOVING_AVERAGE));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.SPIKE_DETECTOR)
//                    , new MarkerShuffleGrouping(Component.SPIKE_DETECTOR)
            );
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
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
