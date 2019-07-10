package applications.general.bolts.gs;


import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import engine.transaction.dedicated.ordered.TxnManagerOrderLockBlocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSBolt_olb extends GSBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;


    public GSBolt_olb(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());

    }

}
