package applications.topology.ol;

import applications.bolts.classifier.ClassifierBolt;
import applications.constants.ClassifierConstants;
import applications.constants.ClassifierConstants.Component;
import applications.constants.ClassifierConstants.Field;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.ClassifierConstants.Conf.CLASSIFIER_THREADS;
import static applications.constants.ClassifierConstants.PREFIX;

public class ClassificationTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationTopology.class);

    public ClassificationTopology(String topologyName, Configuration config) {
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

            builder.setBolt(Component.CLASSIFIER, new ClassifierBolt()
                    , config.getInt(CLASSIFIER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.CLASSIFIER)
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
