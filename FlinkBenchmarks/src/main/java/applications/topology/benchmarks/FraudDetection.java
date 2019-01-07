package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt;
import applications.bolts.fd.FraudPredictorBolt;
import applications.constants.FraudDetectionConstants;
import applications.constants.FraudDetectionConstants.Component;
import applications.constants.FraudDetectionConstants.Field;
import applications.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.FraudDetectionConstants.PREFIX;

public class FraudDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    public FraudDetection(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public FlinkTopology buildTopology() {

        spout.setFields(new Fields(Field.TEXT));
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
//
		builder.setBolt(Component.PARSER, new ParserBolt(parser, new Fields(Field.RECORD_DATA, Field.RECORD_KEY))
                , config.getInt(FraudDetectionConstants.Conf.PARSER_THREADS, 1)).shuffleGrouping(Component.SPOUT);
//
        builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt()
                , config.getInt(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1)
        ).shuffleGrouping(Component.PARSER);

        builder.setBolt(Component.SINK, sink, sinkThreads).shuffleGrouping(Component.PREDICTOR);
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
