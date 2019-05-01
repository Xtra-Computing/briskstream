package applications.bolts.ct;


import applications.param.ct.DepositEvent;
import applications.param.ct.TransactionEvent;
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

    @Override
    protected void deposite_handle(DepositEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());


        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = event.getPid();
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        deposite_request_lock_ahead(event);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);

        _pid = event.getPid();

        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

        END_WAIT_TIME_MEASURE_ACC(thread_Id);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        deposite_request(event);
        END_TP_TIME_MEASURE(thread_Id);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        DEPOSITE_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);//always success..

        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);


    }


    @Override
    protected void transfer_handle(TransactionEvent event, Long timestamp) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());

        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        int _pid = event.getPid();
        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(),   tthread);

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        transfer_request_lock_ahead(event);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);

        _pid = event.getPid();

        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

        END_WAIT_TIME_MEASURE_ACC(thread_Id);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        transfer_request(event);
        END_TP_TIME_MEASURE(thread_Id);


        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

        TRANSFER_CORE(event);

        END_COMPUTE_TIME_MEASURE(thread_Id);
        transactionManager.CommitTransaction(txn_context);//always success..
        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);


    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        rnd = new SplittableRandom(1234);
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock, this.context);
    }


    boolean flag = true;

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        long bid = in.getBID();
        Long timestamp;//in.getLong(1);
        Object event = db.eventManager.get((int) bid);
        if (enable_latency_measurement)
            timestamp = in.getLong(0);
        else
            timestamp = 0L;//

        dispatch_process(event, timestamp);
    }
}
