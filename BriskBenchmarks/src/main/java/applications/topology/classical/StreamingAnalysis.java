package applications.topology.classical;

import applications.constants.streamingAnalysisConstants.Component;
import applications.constants.streamingAnalysisConstants.Field;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.GlobalGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.streamingAnalysisConstants.PREFIX;

public class StreamingAnalysis extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingAnalysis.class);

    public StreamingAnalysis(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        sink = loadSink();

    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

//            builder.setBolt(Component.PARSER, new GeneralParserBolt(parser, new Fields(Field.TIME, Field.VALUE))
//                    , config.getInt(BaseConstants.BaseConf.PARSER_THREADS, 1)
//                    , new ShuffleGrouping(Component.SPOUT));
//
//            builder.setBolt(Component.FILTER,
//                    new FilterBySum()
//                    , config.getInt(streamingAnalysisConstants.Conf.EXECUTOR_THREADS1, 1)
//                    , new ShuffleGrouping(Component.PARSER));
//
//            builder.setBolt(Component.MEDIAN,
//                    new WindowMedian(config.getInt("window"))
//                    , config.getInt(streamingAnalysisConstants.Conf.EXECUTOR_THREADS2, 1)
//                    , new ShuffleGrouping(Component.FILTER));
//
//            builder.setBolt(Component.RANK,
//                    new WindowRank(config.getInt("size_tuple"), config.getInt("window"))
//                    , config.getInt(streamingAnalysisConstants.Conf.EXECUTOR_THREADS3, 1)
//                    , new ShuffleGrouping(Component.PARSER));

            builder.setSink(Component.SINK, sink, sinkThreads
//                    , new GlobalGrouping(Component.MEDIAN)
//                    , new GlobalGrouping(Component.RANK)
//                    , new GlobalGrouping(Component.PARSER)
                    , new GlobalGrouping(Component.SPOUT)
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
