package applications.bolts.mb;


import applications.param.mb.MicroEvent;
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
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_states_partition;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Different from OLB, each executor in SStore has an associated partition id.
 */
public class Bolt_sstore extends MBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Bolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public Bolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    protected void read_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
//        //begin transaction processing.
//
//        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
//        txn_context = new TxnContext(thread_Id, this.fid, event.getBid(), event.getPid());
//
//        //ensures that locks are added in the event sequence order.
//        //ensures all related partitions are locked.
//
////        LOG.info("Task id:" + thread_Id + " works on bid: " + Arrays.toString(bid) + " pid: " + pid);
//        //be careful if there is a deadlock.
//
//
//        BEGIN_WAIT_TIME_MEASURE(thread_Id);
//        int _pid = event.getPid();
//
//        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);
//
//        // //LOG.DEBUG("TaskID: " + thread_Id + " works on PID:" + pid + " bid:" + bid);
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        read_lock_ahead(event, txn_context[(int) (i - _bid)]);
//        END_LOCK_TIME_MEASURE_ACC(thread_Id);
//
//        _pid = event.getPid();
//        LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);
//
//        END_WAIT_TIME_MEASURE_ACC(thread_Id);
//
//        BEGIN_TP_CORE_TIME_MEASURE(thread_Id);
//        read_request(event, txn_context[(int) (i - _bid)]);
//        END_TP_CORE_TIME_MEASURE_TS(txn_context.thread_Id, 1);
//
//        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
//        read_core(event);
//        END_COMPUTE_TIME_MEASURE(thread_Id);
//
//        transactionManager.CommitTransaction(txn_context);
//        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);

    }

    @Override
    protected void write_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {
//        //begin transaction processing.
//
//        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
//        long bid = event.getBid();
//        int pid = event.getPid();
//        int number_of_partitions = event.num_p();
//
//        txn_context = new TxnContext(thread_Id, this.fid, bid, pid);
//
//        BEGIN_WAIT_TIME_MEASURE(thread_Id);
//        //be careful if there is a deadlock.
//        int _pid = pid;
//        LA_LOCK(_pid, number_of_partitions, transactionManager, event.getBid_array(), tthread);
//
//
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        write_lock_ahead(event, txn_context[(int) (i - _bid)]);
//        END_LOCK_TIME_MEASURE_ACC(thread_Id);
//
//        _pid = pid;
//        LA_UNLOCK(_pid, number_of_partitions, transactionManager, tthread);
//
//        END_WAIT_TIME_MEASURE_ACC(thread_Id);
//
//        write_request(event, txn_context[(int) (i - _bid)]);
//
//        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
//
//        write_core(event);
//
//        END_COMPUTE_TIME_MEASURE(thread_Id);
//
//        transactionManager.CommitTransaction(txn_context);
//
//        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);


    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());

        if (!enable_states_partition) {
            LOG.info("Please enable `enable_states_partition` for PAT scheme");
            System.exit(-1);
        }

    }


    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
//        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().GetAndUpdate(0).getExecutorID(), context.getThisTaskId(), context.getGraph());


        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock, this.context);

    }

    protected boolean read_request(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable", String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
        return false;
    }

    protected void read_lock_ahead(MicroEvent Event, TxnContext txnContext) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable", String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_ONLY);
    }


    protected void write_lock_ahead(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i)
            transactionManager.lock_ahead(txn_context, "MicroTable", String.valueOf(Event.getKeys()[i]), Event.getRecord_refs()[i], READ_WRITE);
    }


    protected boolean write_request(MicroEvent Event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            String key = String.valueOf(Event.getKeys()[i]);
            boolean rt = transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable", key, Event.getRecord_refs()[i], READ_WRITE);
        }
        return false;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

    }
}
