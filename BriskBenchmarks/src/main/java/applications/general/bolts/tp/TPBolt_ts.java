package applications.general.bolts.tp;


import applications.general.param.lr.LREvent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import engine.transaction.function.AVG;
import engine.transaction.function.CNT;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.combo_bid_size;
import static applications.CONTROL.enable_app_combo;
import static applications.constants.TP_TxnConstants.Constant.NUM_SEGMENTS;
import static engine.profiler.MeasureTools.*;


/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_ts extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<LREvent> LREvents = new ArrayDeque<>();


    public TPBolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getThisTaskId(), context.getGraph());
    }


    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected long PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException {

        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);

        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            LREvent event = (LREvent) input_event;
            (event).setTimestamp(timestamp);

            REQUEST_CONSTRUCT(event, txnContext);

        }

        return END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);

    }

    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value//holder to be filled up.
                , new AVG(event.getPOSReport().getSpeed())
        );          //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , new CNT(event.getPOSReport().getVid())
        );          //asynchronously return.
        LREvents.add(event);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_SEGMENTS, this.context.getThisComponent().getNumTasks());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {

            int readSize = LREvents.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            REQUEST_REQUEST_CORE();

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, 0, readSize, 0);

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);

            REQUEST_POST();

            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {

            }

            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize);

            LREvents.clear();//clear stored events.

        } else {
            execute_ts_normal(in);
        }
    }


    protected void REQUEST_POST() throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (LREvent event : LREvents) {
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }


    protected void REQUEST_REQUEST_CORE() {

        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(LREvent event) {
        event.count = event.count_value.getRecord().getValue().getInt();
        event.lav = event.speed_value.getRecord().getValue().getDouble();
    }
}
