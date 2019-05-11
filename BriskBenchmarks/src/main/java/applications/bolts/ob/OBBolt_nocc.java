package applications.bolts.ob;

import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerOrderLockBlocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.CONTROL.combo_bid_size;
import static engine.profiler.Metrics.MeasureTools.BEGIN_TRANSACTION_TIME_MEASURE;
import static engine.profiler.Metrics.MeasureTools.END_TOTAL_TIME_MEASURE_ACC;
import static engine.profiler.Metrics.MeasureTools.END_TRANSACTION_TIME_MEASURE;

public class OBBolt_nocc extends OBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_nocc.class);

    public OBBolt_nocc(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
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

//        TXN_PROCESS(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, combo_bid_size);

        END_TOTAL_TIME_MEASURE_ACC(thread_Id, combo_bid_size);
    }
}
