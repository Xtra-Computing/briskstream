package applications.bolts.ct;


import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import engine.transaction.dedicated.ordered.TxnManagerLWM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Combine Read-Write for nocc.
 */
public class CTBolt_lwm extends CTBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;


    public CTBolt_lwm(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());

    }


    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }

}
