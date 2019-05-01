package applications.bolts.mb;


import applications.param.mb.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.storage.SchemaRecordRef;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static engine.profiler.Metrics.MeasureTools.*;

public class Bolt_ts extends GSBolt {


    private static final Logger LOG = LoggerFactory.getLogger(Bolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final ArrayDeque<MicroEvent> EventsHolder = new ArrayDeque<>();

    private int thisTaskId;
    private int writeEvents;

    public Bolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }


    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    private void PRE_TXN_PROCESS(long _bid, Long timestamp) throws DatabaseException, InterruptedException {

        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
            txn_context[(int) (i - _bid)] = new TxnContext(thread_Id, this.fid, i);
            MicroEvent event = (MicroEvent) db.eventManager.get((int) i);
            (event).setTimestamp(timestamp);

            boolean flag = event.READ_EVENT();

            if (flag) {//read
                read_construct(event, txn_context[(int) (i - _bid)]);
            } else {
                write_construct(event, txn_context[(int) (i - _bid)]);
            }
            END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }


    private void read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; i++) {
            //it simply construct the operations and return.
            SchemaRecordRef ref = event.getRecord_refs()[i];
            transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), ref, event.enqueue_time);
        }

        if (enable_speculative) {//TODO: future work.
            //earlier emit
            //collector.emit(event.getBid(), 1, event.getTimestamp());//the tuple is finished.
        }
    }

    private void write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_WriteRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getValues()[i], event.enqueue_time);//asynchronously return.
        }

        //post_process for write events immediately.

        BEGIN_POST_TIME_MEASURE(thread_Id);
        WRITE_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


    private void READ_CORE() throws InterruptedException {
        for (MicroEvent event : EventsHolder) {
            READ_CORE(event);
        }
    }


    private void POST_PROCESS() throws InterruptedException {

        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (MicroEvent event : EventsHolder) {
            READ_POST(event);
        }
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            READ_CORE();

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, 0, EventsHolder.size(), writeEvents);


            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {


            }


            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);


            //post_process for events left-over.

            POST_PROCESS();

            END_TOTAL_TIME_MEASURE_TS(thread_Id, EventsHolder.size() + writeEvents);


            EventsHolder.clear();//all tuples in the EventsHolder are finished.
            if (enable_profile)
                writeEvents = 0;//all tuples in the holder is finished.

        } else {

            //pre stream processing phase..

            BEGIN_PREPARE_TIME_MEASURE(thread_Id);
            Long timestamp;//in.getLong(1);
            if (enable_latency_measurement)
                timestamp = in.getLong(0);
            else
                timestamp = 0L;//

            long _bid = in.getBID();

            END_PREPARE_TIME_MEASURE(thread_Id);

            PRE_TXN_PROCESS(_bid, timestamp);

        }
    }



}
