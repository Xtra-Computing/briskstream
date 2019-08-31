package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt_latency;
import applications.bolts.wc.SplitSentenceBolt;
import applications.bolts.wc.WordCountBolt;
import applications.constants.WordCountConstants;
import applications.constants.WordCountConstants.Component;
import applications.constants.WordCountConstants.Field;
import applications.topology.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.Config;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static applications.constants.WordCountConstants.PREFIX;

public class WordCount_latency extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount_latency.class);

    public WordCount_latency(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
        //  initilize_parser();
    }

    @Override
    public FlinkTopology buildTopology() {

        spout.setFields(
                new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP)
        );

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.PARSER, new ParserBolt_latency(parser,
                        new Fields(Field.WORD, MSG_ID, SYSTEMTIMESTAMP))
//						new Fields(Field.WORD))
                , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1))
                .shuffleGrouping(Component.SPOUT);


        builder.setBolt(Component.SPLITTER, new SplitSentenceBolt()
                , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1))
                .shuffleGrouping(Component.PARSER);

        builder.setBolt(Component.COUNTER, new WordCountBolt()
                , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1))
                .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.COUNTER);
//                .shuffleGrouping(Component.SPOUT)
//                .globalGrouping(Component.SPOUT, Marker_STREAM_ID);

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
