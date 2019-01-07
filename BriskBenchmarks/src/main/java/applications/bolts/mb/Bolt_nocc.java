package applications.bolts.mb;


import applications.param.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.enable_latency_measurement;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
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


    private void read_handle(long bid, Long timestamp) throws InterruptedException, DatabaseException {

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);

        boolean rt;

        if (read_request(event, bid)) {

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            read_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);
            CLEAN_ABORT_TIME_MEASURE(thread_Id);
            transactionManager.CommitTransaction(txn_context);//always success..

            END_TRANSACTION_TIME_MEASURE(thread_Id);
        } else {
            txn_context.is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!read_request(event, bid)) ;
            END_ABORT_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            read_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        }
    }


    private void write_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);


        if (write_request(event, bid)) {
            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            write_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);
            transactionManager.CommitTransaction(txn_context);//always success..
            CLEAN_ABORT_TIME_MEASURE(thread_Id);
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        } else {
            txn_context.is_retry_ = true;
            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!write_request(event, bid)) ;
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

    private boolean read_request(MicroEvent event, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
            if (rt) {
                assert event.getRecord_refs()[i].record != null;
            } else {
                return false;
            }
        }
        return true;
    }


    private boolean write_request(MicroEvent event, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
            if (rt) {
                assert event.getRecord_refs()[i].record != null;
            } else {
                return false;
            }
        }
        return true;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        long bid = in.getBID();
//        boolean flag = in.getBoolean(0);
        boolean flag = next_decision();
        long timestamp;
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        if (flag) {
            read_handle(bid, timestamp);
        } else {
//            long start = System.nanoTime();
            write_handle(bid, timestamp);
//            LOG.info("write handle takes:" + (System.nanoTime() - start));
        }
    }
}
