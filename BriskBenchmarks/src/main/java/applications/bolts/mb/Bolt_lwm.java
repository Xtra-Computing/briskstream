package applications.bolts.mb;


import applications.param.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerLWM;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static applications.CONTROL.enable_latency_measurement;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;

public class Bolt_lwm extends MBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Bolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;
    LinkedList<Long> gap = new LinkedList<>();
    long retry_start = 0;


    public Bolt_lwm(int fid) {
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

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        read_lock_ahead(event, bid);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);

        read_request(event, bid);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        read_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);

        END_TRANSACTION_TIME_MEASURE(thread_Id);
    }


    private void write_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);


        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        write_lock_ahead(event, bid);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.
        END_WAIT_TIME_MEASURE(thread_Id);

        write_request(event, bid);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        write_core(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);

        END_TRANSACTION_TIME_MEASURE(thread_Id);


    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    private void read_lock_ahead(MicroEvent Event, long bid) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable", String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }

    /**
     * @param event
     * @param bid
     * @return
     * @throws DatabaseException
     */
    private boolean read_request(MicroEvent event, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
            assert rt;
        }
        return true;
    }


    private void write_lock_ahead(MicroEvent Event, long bid) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable", String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }

    private void write_request(MicroEvent Event, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            String key = String.valueOf(Event.getKeys()[i]);
            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable", key, Event.getRecord_refs()[i], READ_WRITE);
        }
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        long bid = in.getBID();
        boolean flag = next_decision();
        long timestamp;
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//
        if (flag) {
            read_handle(bid, timestamp);
        } else {
            write_handle(bid, timestamp);
        }
    }

}
