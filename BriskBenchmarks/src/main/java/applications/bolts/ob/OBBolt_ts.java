package applications.bolts.ob;


import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.ToppingEvent;
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
import static applications.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static engine.profiler.Metrics.MeasureTools.*;

public class OBBolt_ts extends OBBolt {
    private static final long serialVersionUID = -589295586738474236L;
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_ts.class);
    private final static double write_useful_time = 1556.713743100476;//write-compute time pre-measured.

    boolean flag = true;
    private int thisTaskId;
    private final ArrayDeque<BuyingEvent> buyingEvents = new ArrayDeque<>();
    private int alertEvents = 0, toppingEvents = 0;


    public OBBolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void buy_handle(BuyingEvent event, Long timestamp) throws DatabaseException {

        BEGIN_READ_HANDLE_TIME_MEASURE(thread_Id);

        buy_request(event, this.fid, event.getBid());

        buyingEvents.add(event);

        END_READ_HANDLE_TIME_MEASURE_TS(thread_Id);

    }

    @Override
    protected void altert_handle(AlertEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        alert_request(event, this.fid, event.getBid());

        if (enable_profile) {
            alertEvents++;//just for record purpose.
        }

        END_WRITE_HANDLE_TIME_MEASURE_TS(thread_Id);


        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is immediately finished.
    }

    @Override
    protected void topping_handle(ToppingEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        topping_request(event, this.fid, event.getBid());
        if (enable_profile) {
            toppingEvents++;//just for record purpose.
        }
        END_WRITE_HANDLE_TIME_MEASURE_TS(thread_Id);

        collector.force_emit(event.getBid(), true, event.getTimestamp());//the tuple is immediately finished.
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thisTaskId = thread_Id;
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());


    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    /**
     * @param event
     * @param fid
     * @param bid
     * @throws DatabaseException
     */
    private void buy_request(BuyingEvent event, int fid, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.
        //it simply construct the operations and return.
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            //it simply constructs the operations and return.
            //condition on itself.
            transactionManager.Asy_ModifyRecord(//TODO: add atomicity preserving later.
                    txn_context,
                    "goods",
                    String.valueOf(event.getItemId()[i]),
                    new DEC(event.getBidQty(i)),
                    new Condition(event.getBidPrice(i), event.getBidQty(i)),
                    event.success
            );
        }
    }

    /**
     * alert price of an item.
     *
     * @param event
     * @param bid
     * @throws DatabaseException
     */
    private void alert_request(AlertEvent event, int fid, long bid) throws DatabaseException {

        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_WriteRecord(txn_context, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
    }

    /**
     * No return is required.
     *
     * @param event
     * @param bid
     * @throws DatabaseException
     */
    private void topping_request(ToppingEvent event, int fid, long bid) throws DatabaseException {

        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_ModifyRecord(txn_context, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        long bid = in.getBID();
        if (in.isMarker()) {

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

            this.collector.ack(in, null);//tell spout, please emit earlier!

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            //Perform computation on each event and emit.
            for (BuyingEvent event : buyingEvents) {

                // measure_end the preconditions
                if (event.success[0]) {
                    collector.force_emit(event.getBid(), new BidingResult(event, true), event.getTimestamp());
                } else {
                    collector.force_emit(event.getBid(), new BidingResult(event, false), event.getTimestamp());
                }
            }

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, write_useful_time, buyingEvents.size(), alertEvents + toppingEvents);

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id, buyingEvents.size() + alertEvents + toppingEvents);

            buyingEvents.clear();//all tuples in the holder is finished.
            if (enable_profile) {
                alertEvents = 0;//all tuples in the holder is finished.
                toppingEvents = 0;
            }

        } else {

            Long timestamp;//in.getLong(1);
            if (enable_latency_measurement) {
                timestamp = in.getLong(0);
            } else {
                timestamp = 0L;//
            }

            Object event = db.eventManager.get((int) bid);

            auth(bid, timestamp);//do nothing for now..
            dispatch_process(event, timestamp);
        }
    }
}
