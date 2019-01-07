package applications.topology.faulttolerance;

import applications.bolts.comm.GeneralParserBolt_FT;
import applications.bolts.wc.SplitSentenceBolt_FT;
import applications.bolts.wc.WordCountBolt_FT;
import applications.constants.WordCountConstants;
import applications.constants.WordCountConstants.Component;
import applications.constants.WordCountConstants.Field;
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

import static applications.constants.WordCount_FTConstants.PREFIX;

public class WordCount_FT extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount_FT.class);

    public WordCount_FT(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT, Field.SYSTEMTIMESTAMP));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.PARSER, new GeneralParserBolt_FT(parser, new Fields(Field.WORD, Field.SYSTEMTIMESTAMP))
                    , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt_FT()
                    , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1)
                    , new ShuffleGrouping(Component.PARSER));
//
            builder.setBolt(Component.COUNTER, new WordCountBolt_FT()
                    , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1)
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD)));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.COUNTER));
//                    , new ShuffleGrouping(Component.SPOUT));

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
