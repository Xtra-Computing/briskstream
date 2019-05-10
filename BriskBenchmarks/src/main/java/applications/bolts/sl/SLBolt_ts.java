package applications.bolts.sl;


import applications.param.TxnEvent;
import applications.param.sl.DepositEvent;
import applications.param.sl.TransactionEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import engine.transaction.function.Condition;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static applications.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static engine.profiler.Metrics.MeasureTools.*;

public class SLBolt_ts extends SLBolt {


    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final static double write_useful_time = 1556.713743100476;//write-compute time pre-measured.
    private final ArrayDeque<TransactionEvent> transactionEvents = new ArrayDeque<>();
    private int depositeEvents;

    public SLBolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ACCOUNTS, this.context.getThisComponent().getNumTasks());

    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {

            int readSize = transactionEvents.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, in.getBID());//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            TRANSFER_REQUEST_CORE();

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, write_useful_time, readSize, depositeEvents);

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);//total txn time

            TRANSFER_REQUEST_POST();

            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + depositeEvents);

            transactionEvents.clear();//all tuples in the holder is finished.

            if (enable_profile) {
                depositeEvents = 0;//all tuples in the holder is finished.
            }

        } else {
            execute_ts_normal(in);
        }
    }

    protected long PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);

        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;

            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);

            if (event instanceof DepositEvent) {
                DEPOSIT_REQUEST_CONSTRUCT((DepositEvent) event, txnContext);
            } else {
                TRANSFER_REQUEST_CONSTRUCT((TransactionEvent) event, txnContext);
            }
        }

        return END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);

    }


    private void TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext) throws DatabaseException {
        String[] srcTable = new String[]{"accounts", "bookEntries"};

        String[] srcID = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId()};

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getSourceAccountId()
                , event.src_account_value,//to be fill up.
                new DEC(event.getAccountTransfer()),
                srcTable, srcID,//condition source, condition id.
                new Condition(
                        event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId()
                , new DEC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getTargetAccountId()
                , event.dst_account_value,//to be fill up.
                new INC(event.getAccountTransfer()),
                srcTable, srcID//condition source, condition id.
                , new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId()
                , new INC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.


        transactionEvents.add(event);
    }

    private void DEPOSIT_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord(txnContext, "accounts", event.getAccountId(), new INC(event.getAccountTransfer()));// read and modify the account itself.
        transactionManager.Asy_ModifyRecord(txnContext, "bookEntries", event.getBookEntryId(), new INC(event.getBookEntryTransfer()));// read and modify the asset itself.

        BEGIN_POST_TIME_MEASURE(thread_Id);
        DEPOSITE_REQUEST_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
        depositeEvents++;
    }

    private void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }

    private void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_CORE(event);
        }
    }


    protected void TRANSFER_REQUEST_CORE(TransactionEvent event) {
        event.transaction_result = new TransactionResult(event, event.success[0],
                event.src_account_value.getRecord().getValues().get(1).getLong(), event.dst_account_value.getRecord().getValues().get(1).getLong());
    }


}
