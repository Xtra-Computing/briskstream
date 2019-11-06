package applications.topology.benchmarks;

import applications.bolts.comm.ParserBolt_latency;
import applications.bolts.wc.SplitSentenceBolt_latency;
import applications.bolts.wc.WordCountBolt_latency;
import applications.topology.BasicTopology;
import constants.WordCountConstants;
import constants.WordCountConstants.Component;
import constants.WordCountConstants.Field;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static constants.WordCountConstants.PREFIX;

public class WordCount_latency extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount_latency.class);

    public WordCount_latency(String topologyName, Config config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public StormTopology buildTopology() {


        spout.setFields(new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.PARSER, new ParserBolt_latency(parser,
                        new Fields(Field.TEXT, MSG_ID, SYSTEMTIMESTAMP))
                , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1))
                .shuffleGrouping(Component.SPOUT);


        builder.setBolt(Component.SPLITTER, new SplitSentenceBolt_latency()
                , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1))
                .shuffleGrouping(Component.PARSER);


        builder.setBolt(Component.COUNTER, new WordCountBolt_latency()
                , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1))
                .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));


        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.COUNTER);

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
