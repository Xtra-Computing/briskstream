package applications.bolts.mb;


import applications.param.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class Bolt_nocc extends MBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Bolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public Bolt_nocc(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void read_handle(MicroEvent event, Long timestamp) throws InterruptedException, DatabaseException {

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        long bid = event.getBid();

        txn_context = new TxnContext(thread_Id, this.fid, bid);


        boolean rt;

        if (read_request_lock(event, bid)) {

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            read_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);
            CLEAN_ABORT_TIME_MEASURE(thread_Id);
            transactionManager.CommitTransaction(txn_context);//always success..

            END_TRANSACTION_TIME_MEASURE(thread_Id);
        } else {
            txn_context.is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!read_request_lock(event, bid)) ;
            END_ABORT_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            read_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        }
    }

    @Override
    protected void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        long bid = event.getBid();
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        if (write_request_lock(event, bid)) {
            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            write_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);
            transactionManager.CommitTransaction(txn_context);//always success..
            CLEAN_ABORT_TIME_MEASURE(thread_Id);
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        } else {
            txn_context.is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!write_request_lock(event, bid)) ;
            END_ABORT_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            write_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        }

    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }





}
