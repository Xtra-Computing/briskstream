package applications.bolts.gs;


import applications.param.mb.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.combo_bid_size;
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class GSBolt_Locks extends GSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_Locks.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSBolt_Locks(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    protected void write_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        boolean success = write_request(event, txn_context[(int) (i - _bid)]);
        END_LOCK_TIME_MEASURE_NOCC(thread_Id);//if success, lock-tp_core-index; if failed, lock -0-index;

        if (success) {
            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        } else {//being aborted.
            txn_context[(int) (i - _bid)].is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!write_request(event, txn_context[(int) (i - _bid)]) && !Thread.currentThread().isInterrupted()) ;
            END_ABORT_TIME_MEASURE_ACC(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        }
    }

    protected void read_txn_process(MicroEvent event, long i, long _bid) throws DatabaseException, InterruptedException {

        boolean success = read_request(event, txn_context[(int) (i - _bid)]);

        if (success) {
            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            READ_CORE(event);
            END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        } else {//being aborted.
            txn_context[(int) (i - _bid)].is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!read_request(event, txn_context[(int) (i - _bid)])) ;
            END_ABORT_TIME_MEASURE_ACC(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            READ_CORE(event);
            END_COMPUTE_TIME_MEASURE_ACC(thread_Id);

            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);//always success..
        }
    }

    private void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            MicroEvent event = (MicroEvent) input_event;
            boolean flag = event.READ_EVENT();
            if (flag) {
                read_txn_process(event, i, _bid);
            } else {
                write_txn_process(event, i, _bid);
            }
        }
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        //pre stream processing phase..

        PRE_EXECUTE(in);

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        TXN_PROCESS(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, combo_bid_size);

        END_TOTAL_TIME_MEASURE_ACC(thread_Id, combo_bid_size);
    }
}
