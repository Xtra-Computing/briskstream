package applications.bolts.ct;


import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
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
public class CTBolt_nocc extends CTBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;
    transient SplittableRandom rnd;
    long retry_start = 0;
    long retry_time = 0;

    public CTBolt_nocc(int fid) {
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


        if (deposite_request(event, bid)) {
            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            DEPOSITE_CORE(event);

            END_COMPUTE_TIME_MEASURE(thread_Id);

            CLEAN_ABORT_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);

        } else {

            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!deposite_request(event, bid) && !Thread.currentThread().isInterrupted()) ;
            END_ABORT_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            DEPOSITE_CORE(event);

            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        }
    }

    boolean flag = true;

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        rnd = new SplittableRandom(1234);

    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    private boolean deposite_request(DepositEvent event, long bid) throws DatabaseException {

        boolean rt = transactionManager.SelectKeyRecord(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);

        rt &= transactionManager.SelectKeyRecord(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

        if (rt) {
            assert event.account_value.record != null && event.asset_value.record != null;
        } else {
            return false;
        }

        return true;
    }


    private boolean transfer_request(TransactionEvent event, long bid) throws DatabaseException {

        boolean rt = transactionManager.SelectKeyRecord(txn_context, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        rt &= transactionManager.SelectKeyRecord(txn_context, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        rt &= transactionManager.SelectKeyRecord(txn_context, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        rt &= transactionManager.SelectKeyRecord(txn_context, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);

        if (rt) {
            assert event.src_account_value.record != null && event.dst_account_value.record != null && event.src_asset_value.record != null && event.dst_asset_value.record != null;
        } else {
            return false;
        }
        return true;
    }

    private void transfer_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        TransactionEvent event = randomTransactionEvent(bid, rnd);
        event.setTimestamp(timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);


        if (transfer_request(event, bid)) {

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            TRANSFER_CORE(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            CLEAN_ABORT_TIME_MEASURE(thread_Id);
            END_TRANSACTION_TIME_MEASURE(thread_Id);


        } else {


            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!transfer_request(event, bid) && !Thread.currentThread().isInterrupted()) ;
            END_ABORT_TIME_MEASURE(thread_Id);


            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            TRANSFER_CORE(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);


            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);

        }
    }

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
