package applications.bolts.gs;


import applications.param.txn.mb.MicroEvent;
import brisk.execution.ExecutionGraph;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerNoLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static engine.profiler.MeasureTools.BEGIN_COMPUTE_TIME_MEASURE;
import static engine.profiler.MeasureTools.END_COMPUTE_TIME_MEASURE_ACC;


/**
 * Combine Read-Write for nocc.
 */
public class GSBolt_nocc extends GSBolt_Locks {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSBolt_nocc(int fid) {
        super(fid);
    }

    @Override
    protected void write_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {

        write_request(event, txn_context[(int) (i - _bid)]);//always success

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        WRITE_CORE(event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
//        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..

    }
    @Override
    protected void read_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {

        read_request(event, txn_context[(int) (i - _bid)]);//always success..

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        READ_CORE(event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
//        transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

}
