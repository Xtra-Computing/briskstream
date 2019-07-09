package applications.topology.ol;

import applications.bolts.learner.EvaluatorBolt;
import applications.bolts.learner.LearnerBolt;
import applications.bolts.learner.VerticalHoeffdingTreeBolt;
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

import static applications.constants.ClassifierConstants.Conf.EVALUTOR_THREADS;
import static applications.constants.ClassifierConstants.Conf.LEARNER_THREADS;
import static applications.constants.ClassifierConstants.PREFIX;


/**
 * Prequential Evaluation task is a scheme in evaluating performance of online classifiers which uses each instance for
 * testing online classifiers model and then it further uses the same instance for training the model(Test-then-train)
 *
 * @author Arinto Murdopo
 * @author shuhaozhang adapt to BriskStream.
 */
public class PrequentialEvaluation extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(PrequentialEvaluation.class);

    private LearnerBolt classifier;
    private EvaluatorBolt evaluator;

    public PrequentialEvaluation(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
        classifier = new VerticalHoeffdingTreeBolt();//for illustration purpose..
        evaluator = new EvaluatorBolt();

    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.LEARNER, classifier
                    , config.getInt(LEARNER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.EVALUATOR, evaluator
                    , config.getInt(EVALUTOR_THREADS, 1)
                    , new ShuffleGrouping(Component.LEARNER));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.EVALUATOR)//seems optional
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
