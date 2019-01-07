package applications.bolts.ct;


import applications.param.DepositEvent;
import applications.param.TransactionEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerSStore;
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
public class CTBolt_sstore extends CTBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;
    transient SplittableRandom rnd;
    long retry_start = 0;
    long retry_time = 0;


    public CTBolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    private void deposite_handle(long[] bid, int pid, int number_of_partitions, long msg_id, long timestamp) throws DatabaseException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid, pid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        DepositEvent event = randomDepositEvent(pid, number_of_partitions, msg_id, timestamp, rnd);//(DepositEvent) in.getValue(0);
        END_PREPARE_TIME_MEASURE(thread_Id);


        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = pid;
        for (int k = 0; k < number_of_partitions; k++) {
            transactionManager.getOrderLock(_pid).blocking_wait(bid[_pid]);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        deposite_request_lock_ahead(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        _pid = pid;
        for (int k = 0; k < number_of_partitions; k++) {
            transactionManager.getOrderLock(_pid).advance();
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }

        END_WAIT_TIME_MEASURE(thread_Id);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        deposite_request(event);
        END_TP_TIME_MEASURE(thread_Id);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        DEPOSITE_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);//always success..

        END_TRANSACTION_TIME_MEASURE(thread_Id);


    }


    private void transfer_handle(long[] bid, int pid, int number_of_partitions, long msg_id, long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid, pid);


        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        TransactionEvent event = randomTransactionEvent(pid, number_of_partitions, msg_id, timestamp, rnd);
        event.setTimestamp(timestamp);
        END_PREPARE_TIME_MEASURE(thread_Id);


        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = pid;
        for (int k = 0; k < number_of_partitions; k++) {
            transactionManager.getOrderLock(_pid).blocking_wait(bid[_pid]);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        transfer_request_lock_ahead(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        _pid = pid;
        for (int k = 0; k < number_of_partitions; k++) {
            transactionManager.getOrderLock(_pid).advance();
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }

        END_WAIT_TIME_MEASURE(thread_Id);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        transfer_request(event, bid[pid]);
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
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        rnd = new SplittableRandom(1234);
    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        context.getGraph().topology.tableinitilizer.loadData(thread_Id, context.getGraph().topology.spinlock, this.context);
    }


    private void deposite_request_lock_ahead(DepositEvent event) throws DatabaseException {

        transactionManager.lock_ahead(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.lock_ahead(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

    }

    private boolean deposite_request(DepositEvent event) throws DatabaseException {

        transactionManager.SelectKeyRecord_noLock(txn_context, "accounts", event.getAccountId(), event.account_value, READ_WRITE);

        transactionManager.SelectKeyRecord_noLock(txn_context, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);

        assert event.account_value.record != null && event.asset_value.record != null;

        return true;
    }

    private void transfer_request_lock_ahead(TransactionEvent event) throws DatabaseException {
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
        long[] partitionBID = in.getPartitionBID();

//        boolean flag = in.getBoolean(0);
        int pid = in.getInt(1);//start point.
        int number_of_partitions = in.getInt(2);
        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        if (flag) {
            deposite_handle(partitionBID, pid, number_of_partitions, bid, timestamp);
            flag = false;
        } else {
            transfer_handle(partitionBID, pid, number_of_partitions, bid, timestamp);
            flag = true;
        }
    }
}
