package applications.bolts.lr.txn;


 
import applications.param.lr.LREvent;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for TStream.
 */
public class TP_TStream extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TP_TStream.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TP_TStream(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void read_handle(LREvent event, Long timestamp) throws InterruptedException, DatabaseException {
    }

    @Override
    protected void write_handle(LREvent event, Long timestamp) throws DatabaseException, InterruptedException {
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }
}
