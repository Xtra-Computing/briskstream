package applications.bolts.gs;


import applications.param.txn.mb.MicroEvent;
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

import static applications.CONTROL.enable_states_partition;
import static engine.profiler.MeasureTools.*;


/**
 * Different from OLB, each executor in SStore has an associated partition id.
 */
public class GSBolt_sstore extends GSBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public GSBolt_sstore(int fid) {
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
    protected void LAL_PROCESS(long _bid) throws DatabaseException {


        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);
            MicroEvent event = (MicroEvent) input_event;
            int _pid = event.getPid();

            BEGIN_WAIT_TIME_MEASURE(thread_Id);
            //ensures that locks are added in the input_event sequence order.
            LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), _bid, tthread);

            BEGIN_LOCK_TIME_MEASURE(thread_Id);

            LAL(event, i, _bid);

            long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);

            LA_UNLOCK(_pid, event.num_p(), transactionManager, _bid, tthread);

            END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
        }
    }

}
