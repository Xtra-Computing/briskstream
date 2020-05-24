package applications.spout;

import applications.spout.generator.WCGenerator;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.DEFAULT_STREAM_ID;

public class WCGeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(WCGeneratorSpout.class);
    private static final long serialVersionUID = 7738169734935576086L;
    private WCGenerator Generator;

    public WCGeneratorSpout() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        String delimiter = ",";
        int number_of_words = 10;
        Generator = new WCGenerator(this.getContext().getThisTaskId(), number_of_words, delimiter);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit(DEFAULT_STREAM_ID, Generator.next());
    }
}
