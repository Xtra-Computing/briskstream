package applications.bolts.sl;


import applications.param.sl.DepositEvent;
import applications.param.sl.TransactionEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static applications.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static engine.profiler.Metrics.MeasureTools.*;

public class SLBolt_ts_nopush extends SLBolt_ts {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts_nopush.class);

    ArrayDeque<DepositEvent> depositeEvents;

    public SLBolt_ts_nopush(int fid) {
        super(fid);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ACCOUNTS, this.context.getThisComponent().getNumTasks());
        depositeEvents = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {

            int readSize = transactionEvents.size();
            int depoSize = depositeEvents.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, in.getBID());//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            TRANSFER_REQUEST_CORE();

            DEPOSITE_REQUEST_CORE();


            END_COMPUTE_TIME_MEASURE_TS(thread_Id, 0, readSize + depoSize, 0);

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);//total txn time

            TRANSFER_REQUEST_POST();

            DEPOSITE_REQUEST_POST();
            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + depoSize);

            transactionEvents.clear();//all tuples in the holder is finished.
            depositeEvents.clear();


        } else {
            execute_ts_normal(in);
        }
    }


    protected void TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext) throws DatabaseException {

        transactionManager.Asy_ReadRecord(txnContext,
                "accounts",
                event.getSourceAccountId()
                , event.src_account_value,//to be fill up.
                event.enqueue_time);          //asynchronously return.

        transactionManager.Asy_ReadRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId(),
                event.src_asset_value
                , event.enqueue_time);   //asynchronously return.

        transactionManager.Asy_ReadRecord(txnContext,
                "accounts",
                event.getTargetAccountId()
                , event.dst_account_value,//to be fill up.
                event.enqueue_time);          //asynchronously return.

        transactionManager.Asy_ReadRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                event.dst_asset_value
                , event.enqueue_time);   //asynchronously return.


        transactionEvents.add(event);
    }

    protected void DEPOSITE_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.Asy_ReadRecord(txnContext, "accounts", event.getAccountId(), event.account_value, event.enqueue_time);
        transactionManager.Asy_ReadRecord(txnContext, "bookEntries", event.getBookEntryId(), event.asset_value, event.enqueue_time);
        depositeEvents.add(event);
    }


    private void DEPOSITE_REQUEST_CORE() {
        for (DepositEvent event : depositeEvents) {
            DEPOSITE_REQUEST_CORE(event);
        }
    }

    private void DEPOSITE_REQUEST_POST() throws InterruptedException {
        for (DepositEvent event : depositeEvents) {
            DEPOSITE_REQUEST_POST(event);
        }
    }

    protected void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }

    protected void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_CORE(event);
        }
    }


}
