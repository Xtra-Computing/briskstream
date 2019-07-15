package applications.ml.spout;

import applications.ml.datatypes.events.InstanceContentEvent;
import applications.ml.datatypes.instance.Instance;
import applications.util.datatypes.StreamValues;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.DEFAULT_STREAM_ID;

public class LableStreamGeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LableStreamGeneratorSpout.class);
    private static final long serialVersionUID = 7738169734935576086L;
    private RandomTreeGenerator Generator;
    private Instance firstInstance;
    private boolean finished = false;
    private int numInstanceSent = 0;
    private boolean isInited = false;

    public LableStreamGeneratorSpout() {
        super(LOG);
//        preqSource.setMaxNumInstances(instanceLimitOption.getValue());
//        preqSource.setSourceDelay(sourceDelayOption.getValue());
//        preqSource.setDelayBatchSize(batchDelayOption.getValue());

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numContestants = 100;
        Generator = new RandomTreeGenerator(this.getContext().getThisTaskId(), numContestants);
        Generator.prepareForUse();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
        InstanceContentEvent contentEvent = null;
        if (hasReachedEndOfStream()) {//currently not in use.
            contentEvent = new InstanceContentEvent(-1, firstInstance, false, true);
            contentEvent.setLast(true);
            // set finished status _after_ tagging last event
            finished = true;
        } else if (hasNext()) {
            numInstanceSent++;
            contentEvent = new InstanceContentEvent(numInstanceSent, nextInstance(), true, true);

//            // first call to this method will trigger the timer
//            if (schedule == null && delay > 0) {
//                schedule = timer.scheduleWithFixedDelay(new DelayTimeoutHandler(this), delay, delay,
//                        TimeUnit.MICROSECONDS);
//            }
        }
        //return contentEvent;
        collector.emit(DEFAULT_STREAM_ID, new StreamValues(contentEvent));//send out a batch of instances.
    }

    private Instance nextInstance() {
        if (this.isInited) {
            return Generator.nextInstance();
        } else {
            this.isInited = true;
            return firstInstance;
        }
    }

    private boolean hasNext() {
        return true;//TODO
    }

    private boolean hasReachedEndOfStream() {
        return false;//TODO
    }
}
