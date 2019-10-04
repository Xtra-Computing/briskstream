package applications.bolts.transactional.ob;

import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import engine.transaction.dedicated.ordered.TxnManagerOrderLockBlocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OBBolt_olb extends OBBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_olb.class);

    public OBBolt_olb(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }
}
