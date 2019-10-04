package applications.bolts.transactional.ob;

import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.ToppingEvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerNoLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.CONTROL.combo_bid_size;
import static engine.profiler.MeasureTools.BEGIN_COMPUTE_TIME_MEASURE;
import static engine.profiler.MeasureTools.END_COMPUTE_TIME_MEASURE_ACC;

public class OBBolt_nocc extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_nocc.class);

    public OBBolt_nocc(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            if (input_event instanceof BuyingEvent) {
                buying_txn_process((BuyingEvent) input_event, i, _bid);
            } else if (input_event instanceof AlertEvent) {
                alert_txn_process((AlertEvent) input_event, i, _bid);
            } else {
                topping_txn_process((ToppingEvent) input_event, i, _bid);
            }
        }
    }

    private void topping_txn_process(ToppingEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TOPPING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        TOPPING_REQUEST_CORE(input_event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
    }

    private void alert_txn_process(AlertEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        ALERT_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        ALERT_REQUEST_CORE(input_event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
    }

    private void buying_txn_process(BuyingEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        BUYING_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        BUYING_REQUEST_CORE(input_event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
    }
}
