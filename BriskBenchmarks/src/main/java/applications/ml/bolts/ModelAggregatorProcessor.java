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

/**
 * Model Aggegator Processor consists of the decision tree model. It connects to local-statistic PI via attribute stream
 * and control stream. Model-aggregator PI sends the split instances via attribute stream and it sends control messages
 * to ask local-statistic PI to perform computation via control stream.
 *
 * Model-aggregator PI sends the classification result via result stream to an evaluator PI for classifier or other
 * destination PI. The calculation results from local statistic arrive to the model-aggregator PI via computation-result
 * stream.
 *
 * @author Arinto Murdopo
 *
 */
public class ModelAggregatorProcessor extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);
    private List<InstanceContentEvent> contentEventList;
    private int waitingInstances = 0;

    private int batchSize = 10;//buffering multiple events before sending out.

    public ModelAggregatorProcessor() {
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

//        // Poll the blocking queue shared between ModelAggregator and the time-out threads
//        Long timedOutSplitId = timedOutSplittingNodes.poll();
//        if (timedOutSplitId != null) { // time out has been reached!
//            SplittingNodeInfo splittingNode = splittingNodes.get(timedOutSplitId);
//            if (splittingNode != null) {
//                this.splittingNodes.remove(timedOutSplitId);
//                this.continueAttemptToSplit(splittingNode.activeLearningNode, splittingNode.foundNode);
//
//            }
//
//        }
//
//        // Receive a new instance from source
//        if (event instanceof InstancesContentEvent) {
//            InstancesContentEvent instancesEvent = (InstancesContentEvent) event;
//            this.processInstanceContentEvent(instancesEvent);
//            // Send information to local-statistic PI
//            // for each of the nodes
//            if (this.foundNodeSet != null) {
//                for (FoundNode foundNode : this.foundNodeSet) {
//                    ActiveLearningNode leafNode = (ActiveLearningNode) foundNode.getNode();
//                    AttributeBatchContentEvent[] abce = leafNode.getAttributeBatchContentEvent();
//                    if (abce != null) {
//                        for (int i = 0; i < this.dataset.numAttributes() - 1; i++) {
//                            this.sendToAttributeStream(abce[i]);
//                        }
//                    }
//                    leafNode.setAttributeBatchContentEvent(null);
//                    // this.sendToControlStream(event); //split information
//                    // See if we can ask for splits
//                    if (!leafNode.isSplitting()) {
//                        double weightSeen = leafNode.getWeightSeen();
//                        // check whether it is the time for splitting
//                        if (weightSeen - leafNode.getWeightSeenAtLastSplitEvaluation() >= this.gracePeriod) {
//                            attemptToSplit(leafNode, foundNode);
//                        }
//                    }
//                }
//            }
//            this.foundNodeSet = null;
//        } else if (event instanceof LocalResultContentEvent) {
//            LocalResultContentEvent lrce = (LocalResultContentEvent) event;
//            Long lrceSplitId = lrce.getSplitId();
//            SplittingNodeInfo splittingNodeInfo = splittingNodes.get(lrceSplitId);
//
//            if (splittingNodeInfo != null) { // if null, that means
//                // activeLearningNode has been
//                // removed by timeout thread
//                ActiveLearningNode activeLearningNode = splittingNodeInfo.activeLearningNode;
//
//                activeLearningNode.addDistributedSuggestions(lrce.getBestSuggestion(), lrce.getSecondBestSuggestion());
//
//                if (activeLearningNode.isAllSuggestionsCollected()) {
//                    splittingNodeInfo.scheduledFuture.cancel(false);
//                    this.splittingNodes.remove(lrceSplitId);
//                    this.continueAttemptToSplit(activeLearningNode, splittingNodeInfo.foundNode);
//                }
//            }
//        }
    }

}
