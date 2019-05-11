package applications.bolts.sl;


import applications.param.sl.DepositEvent;
import applications.param.sl.TransactionEvent;
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
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for nocc.
 */
public class SLBolt_nocc extends SLBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public SLBolt_nocc(int fid) {
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
        //pre stream processing phase..

        PRE_EXECUTE(in);

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        TXN_PROCESS(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, combo_bid_size);

        END_TOTAL_TIME_MEASURE_ACC(thread_Id, combo_bid_size);
    }

    private void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            if (input_event instanceof DepositEvent) {
                depo_txn_process((DepositEvent) input_event, i, _bid);
            } else {
                trans_txn_process((TransactionEvent) input_event, i, _bid);
            }
        }
    }

    private void trans_txn_process(TransactionEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        TRANSFER_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success
        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        TRANSFER_REQUEST_CORE(input_event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
    }

    private void depo_txn_process(DepositEvent input_event, long i, long _bid) throws DatabaseException, InterruptedException {
        DEPOSITE_REQUEST(input_event, txn_context[(int) (i - _bid)]);//always success

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        DEPOSITE_REQUEST_CORE(input_event);
        END_COMPUTE_TIME_MEASURE_ACC(thread_Id);
    }

}
