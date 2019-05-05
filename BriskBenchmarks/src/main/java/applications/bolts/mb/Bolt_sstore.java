package applications.bolts.mb;


import applications.param.mb.MicroEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerSStore;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.CONTROL.combo_bid_size;
import static applications.CONTROL.enable_states_partition;
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Different from OLB, each executor in SStore has an associated partition id.
 */
public class Bolt_sstore extends Bolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(Bolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public Bolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
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

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);
            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);
            int _pid = event.getPid();

            BEGIN_WAIT_TIME_MEASURE(thread_Id);
            //ensures that locks are added in the event sequence order.
            LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), tthread);

            BEGIN_LOCK_TIME_MEASURE(thread_Id);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                read_lock_ahead(event, txn_context[(int) (i - _bid)]);
            } else {
                write_lock_ahead(event, txn_context[(int) (i - _bid)]);
            }

            long lock_time_measure =  END_LOCK_TIME_MEASURE_ACC(thread_Id);

            LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);

            END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
        }
    }

}
