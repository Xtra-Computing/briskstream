package applications.bolts.ct;


import applications.param.DepositEvent;
import applications.param.TransactionEvent;
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

import static applications.CONTROL.enable_latency_measurement;
import static applications.CONTROL.enable_profile;
import static engine.profiler.Metrics.MeasureTools.*;

public class CTBolt_ts extends CTBolt {


    private static final Logger LOG = LoggerFactory.getLogger(CTBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final static double write_useful_time = 1556.713743100476;//write-compute time pre-measured.
    private final ArrayDeque<TransactionEvent> transactionEvents = new ArrayDeque<>();
    private int depositeEvents;

    public CTBolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void deposite_handle(DepositEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_READ_HANDLE_TIME_MEASURE(thread_Id);

        deposite_request(event, event.getBid());

        if (enable_profile) {
            depositeEvents++;//just for record purpose.
        }

        END_READ_HANDLE_TIME_MEASURE(thread_Id);

        collector.force_emit(event.getBid(), null, event.getTimestamp());
    }

    /**
     * @param event
     * @param bid
     * @throws DatabaseException
     */
    private void deposite_request(DepositEvent event, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord(txn_context, "accounts", event.getAccountId(), new INC(event.getAccountTransfer()));// read and modify the account itself.
        transactionManager.Asy_ModifyRecord(txn_context, "bookEntries", event.getBookEntryId(), new INC(event.getBookEntryTransfer()));// read and modify the asset itself.
    }

    @Override
    protected void transfer_handle(TransactionEvent event, Long timestamp) throws DatabaseException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);


        transfer_request(event, event.getBid());

        transactionEvents.add(event);

        END_WRITE_HANDLE_TIME_MEASURE(thread_Id);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());

    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    /**
     * @param bid
     * @throws DatabaseException
     */
    private void transfer_request(TransactionEvent event, long bid) throws DatabaseException {

        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.

        String[] srcTable = new String[]{"accounts", "bookEntries"};

        String[] srcID = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId()};

        transactionManager.Asy_ModifyRecord_Read(txn_context,
                "accounts",
                event.getSourceAccountId()
                , event.src_account_value,
                new DEC(event.getAccountTransfer()),
                srcTable, srcID,//condition source, condition id.
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txn_context,
                "bookEntries", event.getSourceBookEntryId()
                , new DEC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txn_context,
                "accounts",
                event.getTargetAccountId()
                , event.dst_account_value,
                new INC(event.getAccountTransfer()),
                srcTable, srcID//condition source, condition id.
                , new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txn_context,
                "bookEntries",
                event.getTargetBookEntryId()
                , new INC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.

    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        long bid = in.getBID();
        if (in.isMarker()) {

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, this.fid, bid);//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

//            final Marker marker = in.getMarker();
            this.collector.ack(in, null);//tell spout, please emit earlier!

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            //Perform computation on each event and emit.
            for (TransactionEvent event : transactionEvents) {
                final long sourceAccountBalance = event.src_account_value.record.getValues().get(1).getLong();//already updated in the engine.
                final long targetAccountBalance = event.dst_account_value.record.getValues().get(1).getLong();//already updated in the engine.

                // measure_end the preconditions
                if (event.success[0]) {
                    collector.force_emit(event.getBid(), new TransactionResult(event, true, sourceAccountBalance, targetAccountBalance), event.getTimestamp());
                } else {
                    collector.force_emit(event.getBid(), new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance), event.getTimestamp());
                }
            }
            transactionEvents.clear();//all tuples in the holder is finished.

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, write_useful_time, depositeEvents + transactionEvents.size());


            if (enable_profile) {
                depositeEvents = 0;//all tuples in the holder is finished.
            }

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);

        } else {
            Long timestamp;//in.getLong(1);
            if (enable_latency_measurement) {
                timestamp = in.getLong(0);
            } else {
                timestamp = 0L;//
            }

            Object event = db.eventManager.get((int) bid);

            dispatch_process(event, timestamp);
        }
    }
}
