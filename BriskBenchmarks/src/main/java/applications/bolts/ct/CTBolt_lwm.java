package applications.bolts.ct;


import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerLWM;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SplittableRandom;

import static applications.CONTROL.enable_latency_measurement;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class CTBolt_lwm extends CTBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt_lwm.class);
    private static final long serialVersionUID = -5968750340131744744L;
    transient SplittableRandom rnd;
    long retry_start = 0;
    long retry_time = 0;


    public CTBolt_lwm(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    private void deposite_handle(long bid, Long timestamp) throws DatabaseException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        DepositEvent event = randomDepositEvent(bid, rnd);//(DepositEvent) in.getValue(0);
        END_PREPARE_TIME_MEASURE(thread_Id);

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        deposite_request_lock_ahead(event, bid);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);


        BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
        deposite_request(event, bid);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        DEPOSITE_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);//always success..

        END_TRANSACTION_TIME_MEASURE(thread_Id);

    }


    private void transfer_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        TransactionEvent event = randomTransactionEvent(bid, rnd);
        event.setTimestamp(timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        transfer_request_lock_ahead(event, bid);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        transfer_request(event, bid);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        TRANSFER_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);


    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLWM(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        rnd = new SplittableRandom(1234);

    }


    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }

    private void deposite_request_lock_ahead(DepositEvent event, long bid) throws DatabaseException {

        transactionManager.lock_ahead(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

    }

    private boolean deposite_request(DepositEvent event, long bid) throws DatabaseException {

        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);

        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

        assert event.account_value.record != null && event.asset_value.record != null;

        return true;
    }

    private void transfer_request_lock_ahead(TransactionEvent event, long bid) throws DatabaseException {
        transactionManager.lock_ahead(txn_context, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);
    }

    private boolean transfer_request(TransactionEvent event, long bid) throws DatabaseException {


        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);


        assert event.src_account_value.record != null && event.dst_account_value.record != null && event.src_asset_value.record != null && event.dst_asset_value.record != null;
        return true;
    }

    boolean flag = true;

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        long bid = in.getBID();
//        boolean flag = in.getBoolean(0);
        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        if (flag) {
            deposite_handle(bid, timestamp);
            flag = false;
        } else {
            transfer_handle(bid, timestamp);
            flag = true;
        }
    }
}
