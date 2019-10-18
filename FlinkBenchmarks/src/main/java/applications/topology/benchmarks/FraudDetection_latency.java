package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt_latency;
import applications.bolts.fd.FraudPredictor_latencyBolt;
import constants.FraudDetectionConstants;
import constants.FraudDetectionConstants.Component;
import constants.FraudDetectionConstants.Field;
import applications.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static constants.FraudDetectionConstants.PREFIX;

public class FraudDetection_latency extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection_latency.class);

    public FraudDetection_latency(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public FlinkTopology buildTopology() {

        spout.setFields(
                new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP)
        );

        builder.setSpout(Component.SPOUT, spout, spoutThreads);
//
        builder.setBolt(Component.PARSER,
                new ParserBolt_latency(parser,
                        new Fields(Field.RECORD_DATA, Field.RECORD_KEY, MSG_ID, SYSTEMTIMESTAMP))
                , config.getInt(FraudDetectionConstants.Conf.PARSER_THREADS, 1))
                .shuffleGrouping(Component.SPOUT);
//
        builder.setBolt(Component.PREDICTOR, new FraudPredictor_latencyBolt()
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
