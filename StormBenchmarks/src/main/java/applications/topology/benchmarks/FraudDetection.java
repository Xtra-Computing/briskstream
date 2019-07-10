package applications.general.topology.benchmarks;

import applications.general.bolts.comm.ParserBolt;
import applications.general.bolts.fd.FraudPredictorBolt;
import applications.constants.FraudDetectionConstants;
import applications.constants.FraudDetectionConstants.Component;
import applications.constants.FraudDetectionConstants.Field;
import applications.general.topology.BasicTopology;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
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
	public StormTopology buildTopology() {

		spout.setFields(new Fields(Field.TEXT));
//        spout.setFields(Marker_STREAM_ID, new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP));

		builder.setSpout(Component.SPOUT, spout, spoutThreads);

		builder.setBolt(Component.PARSER, new ParserBolt(parser
						, new Fields(Field.RECORD_DATA, Field.RECORD_KEY)
				)
				, config.getInt(FraudDetectionConstants.Conf.PARSER_THREADS, 1))
				.shuffleGrouping(Component.SPOUT)
//                .shuffleGrouping(Component.SPOUT, Marker_STREAM_ID);
		;
		builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt()
				, config.getInt(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1))
				.shuffleGrouping(Component.PARSER)
//                .shuffleGrouping(Component.PARSER, Marker_STREAM_ID);
		;
		builder.setBolt(Component.SINK, sink, sinkThreads)
				.shuffleGrouping(Component.PREDICTOR)
//                .globalGrouping(Component.PREDICTOR, Marker_STREAM_ID);
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
