package applications.topology;

import applications.bolts.comm.FraudDetectionParserBolt;
import applications.bolts.fd.FraudPredictorBolt;
import applications.constants.FraudDetectionConstants;
import applications.constants.FraudDetectionConstants.Component;
import applications.constants.FraudDetectionConstants.Field;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.FraudDetectionConstants.PREFIX;

public class FraudDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    public FraudDetection(String topologyName, Configuration config) {
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
//
            builder.setBolt(Component.PARSER, new FraudDetectionParserBolt(parser,
                            new Fields(Field.ENTITY_ID, Field.RECORD_DATA))
                    , config.getInt(FraudDetectionConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));
//
            builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt()
                    , config.getInt(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1)
                    , new FieldsGrouping(Component.PARSER, new Fields(Field.ENTITY_ID)));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.PREDICTOR)
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
