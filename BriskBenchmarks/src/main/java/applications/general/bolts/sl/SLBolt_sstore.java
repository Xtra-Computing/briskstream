package applications.general.bolts.sl;


import applications.general.param.TxnEvent;
import applications.general.param.sl.DepositEvent;
import applications.general.param.sl.TransactionEvent;
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

import static engine.profiler.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_sstore extends SLBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;


    public SLBolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
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
    protected void LAL_PROCESS(long _bid) throws DatabaseException {
        int _combo_bid_size = 1;
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;

            int _pid = (event).getPid();

            BEGIN_WAIT_TIME_MEASURE(thread_Id);
            //ensures that locks are added in the input_event sequence order.
            LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), _bid, tthread);

            BEGIN_LOCK_TIME_MEASURE(thread_Id);

            if (event instanceof DepositEvent) {
                DEPOSITE_LOCK_AHEAD((DepositEvent) event, txn_context[(int) (i - _bid)]);
            } else {
                TRANSFER_LOCK_AHEAD((TransactionEvent) event, txn_context[(int) (i - _bid)]);
            }

            LA_UNLOCK(_pid, event.num_p(), transactionManager, _bid, tthread);

            long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);

            END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
        }
    }


}
