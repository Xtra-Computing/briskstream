package applications.ml.bolts;

import applications.ml.datatypes.events.InstanceContentEvent;
import applications.ml.datatypes.events.InstancesContentEvent;
import applications.util.datatypes.StreamValues;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static applications.Constants.DEFAULT_STREAM_ID;

public class FilterProcessor extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);
    private List<InstanceContentEvent> contentEventList;
    private int waitingInstances = 0;

    private int batchSize = 10;//buffering multiple events before sending out.

    public FilterProcessor() {
        super(LOG, new HashMap<>());
    }

    /**
     * Initilize this Learner.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     */
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        contentEventList = new LinkedList<>();
        LOG.info("Successfully instantiating Filter");
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
        // Receive a new instance from source
        InstanceContentEvent instanceContentEvent = (InstanceContentEvent) in.getValue(0);//assuming only single object is passed here.

        this.contentEventList.add(instanceContentEvent);
        this.waitingInstances++;

        if (this.waitingInstances == this.batchSize || instanceContentEvent.isLastEvent()) {
            // Send Instances
            InstancesContentEvent outputEvent = new InstancesContentEvent();
            while (!this.contentEventList.isEmpty()) {
                InstanceContentEvent ice = this.contentEventList.remove(0);
                outputEvent.add(ice.getInstanceContent());
            }
            this.waitingInstances = 0;
            collector.emit(DEFAULT_STREAM_ID, new StreamValues(outputEvent));

//            if (this.delay > 0) { //TODO: Do we really need to delay the execution here??
//                try {
//                    Thread.sleep(this.delay);
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                }
//            }
        }

    }

}
