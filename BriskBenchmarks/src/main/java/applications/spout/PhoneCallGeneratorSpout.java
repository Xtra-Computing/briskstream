package applications.spout;

import applications.spout.generator.PhoneCallGenerator;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.DEFAULT_STREAM_ID;

public class PhoneCallGeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneCallGeneratorSpout.class);
    private static final long serialVersionUID = 7738169734935576086L;
    private PhoneCallGenerator callGenerator;

    public PhoneCallGeneratorSpout() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numContestants = 100;
        callGenerator = new PhoneCallGenerator(this.getContext().getThisTaskId(), numContestants);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit_bid(DEFAULT_STREAM_ID, callGenerator.next());
    }
}
