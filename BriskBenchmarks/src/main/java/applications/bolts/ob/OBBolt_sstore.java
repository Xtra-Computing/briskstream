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
import engine.transaction.dedicated.ordered.TxnManagerSStore;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public class OBBolt_sstore extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_sstore.class);

    public OBBolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void topping_handle(ToppingEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = event.getPid();

        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Topping_REQUEST_LA(event);
        long lock_time_measure =END_LOCK_TIME_MEASURE_ACC(thread_Id);

        _pid = event.getPid();

        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        Topping_REQUEST(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        Topping_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);

    }

    @Override
    protected void altert_handle(AlertEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = event.getPid();
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Alert_REQUEST_LA(event);
        long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);

        _pid = event.getPid();

        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);


        BEGIN_TP_TIME_MEASURE(thread_Id);
        Alert_REQUEST(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        Alert_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id);
    }

    @Override
    protected void buy_handle(BuyingEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);

        int _pid = event.getPid();
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);


        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        Buying_REQUEST_LA(event);
        long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);

        _pid = event.getPid();
        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);


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
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock, this.context);
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        long bid = in.getBID();
        Long timestamp;//in.getLong(1);
        Object event = db.eventManager.get((int) bid);
        if (enable_latency_measurement) {
            timestamp = in.getLong(0);
        }
        else
            timestamp = 0L;//

        auth(bid, timestamp);//do nothing for now..

        dispatch_process(event, timestamp);
    }
}
