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
import engine.transaction.dedicated.ordered.TxnManagerOrderLockBlocking;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public class OBBolt_olb extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_olb.class);

    public OBBolt_olb(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    protected void topping_handle(ToppingEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());


        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Topping_REQUEST_LA(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        Topping_REQUEST(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        Topping_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);

    }

    private void altert_handle(AlertEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Alert_REQUEST_LA(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        Alert_REQUEST(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        Alert_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);
    }


    protected void buy_handle(BuyingEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        transactionManager.getOrderLock().blocking_wait(event.getBid());//ensures that locks are added in the event sequence order.

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Buying_REQUEST_LA(event);
        END_LOCK_TIME_MEASURE(thread_Id);

        transactionManager.getOrderLock().advance();//ensures that locks are added in the event sequence order.

        END_WAIT_TIME_MEASURE(thread_Id);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        Buying_REQUEST(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        Buying_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        long bid = in.getBID();

        Object event = db.eventManager.get((int) bid);

        Long timestamp;//in.getLong(1);

        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        auth(bid, timestamp);//do nothing for now..

        if (event instanceof BuyingEvent) {
            buy_handle((BuyingEvent) event, timestamp);//buy item at certain price.
        } else if (event instanceof AlertEvent) {
            altert_handle((AlertEvent) event, timestamp);//alert price
        } else if (event instanceof ToppingEvent) {
            topping_handle((ToppingEvent) event, timestamp);//topping qty
        }


    }


}
