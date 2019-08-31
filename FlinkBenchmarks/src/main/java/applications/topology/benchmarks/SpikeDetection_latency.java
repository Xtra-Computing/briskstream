package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt_latency;
import applications.bolts.sd.MovingAverageBolt_latency;
import applications.bolts.sd.SpikeDetectionBolt_latency;
import applications.constants.SpikeDetectionConstants;
import applications.constants.SpikeDetectionConstants.Component;
import applications.constants.SpikeDetectionConstants.Field;
import applications.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static applications.constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import static applications.constants.SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS;
import static applications.constants.SpikeDetectionConstants.PREFIX;

public class SpikeDetection_latency extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection_latency.class);

    public SpikeDetection_latency(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public FlinkTopology buildTopology() {

        spout.setFields(new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP));
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.PARSER,
                new ParserBolt_latency(parser
                        , new Fields(Field.DEVICE_ID, Field.TIMESTAMP, Field.VALUE, MSG_ID, SYSTEMTIMESTAMP))
                , config.getInt(SpikeDetectionConstants.Conf.PARSER_THREADS, 1))
                .shuffleGrouping(Component.SPOUT);

        builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt_latency(),
                config.getInt(MOVING_AVERAGE_THREADS, 1))
                .fieldsGrouping(Component.PARSER, new Fields(Field.DEVICE_ID));

        builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt_latency(),
                config.getInt(SPIKE_DETECTOR_THREADS, 1))
                .shuffleGrouping(Component.MOVING_AVERAGE);

        builder.setBolt(Component.SINK, sink, sinkThreads
//                    , new shuffleGrouping(Component.SPOUT)
        ).shuffleGrouping(Component.SPIKE_DETECTOR);
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
