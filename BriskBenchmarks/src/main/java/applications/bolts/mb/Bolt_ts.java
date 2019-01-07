package applications.bolts.mb;


import applications.param.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.storage.datatype.DataBox;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.*;
import static engine.profiler.Metrics.MeasureTools.*;

public class Bolt_ts extends MBBolt {


    private static final Logger LOG = LoggerFactory.getLogger(Bolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final ArrayDeque<MicroEvent> readEventHolder = new ArrayDeque<>();

    private int thisTaskId;
    private int writeEvents;

    public Bolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    private void read_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {


        BEGIN_READ_HANDLE_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);
        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        END_PREPARE_TIME_MEASURE_TS(thread_Id);

        read_requests(event, this.fid, bid);

        readEventHolder.add(event);//mark the tuple as ``in-complete"
        END_READ_HANDLE_TIME_MEASURE(thread_Id);

        if (enable_speculative) {
            //earlier emit
            collector.force_emit(event.getBid(), event.getEmit_timestamp(), 1);//the tuple is finished.
        }
    }

    private void write_handle(long bid, Long timestamp) throws InterruptedException, DatabaseException {

        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);
        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        END_PREPARE_TIME_MEASURE_TS(thread_Id);


        write_requests(event.enqueue_time, event.index_time, event.getKeys(), event.getValues(), event.getBid());

        if (enable_profile)
            writeEvents++;//just for record purpose.

        collector.force_emit(event.getBid(), event.getEmit_timestamp());//the tuple is immediately finished.

        END_WRITE_HANDLE_TIME_MEASURE(thread_Id);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }

    /**
     * @param event
     * @param fid
     * @param bid
     * @throws DatabaseException
     */
    private void read_requests(MicroEvent event, int fid, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; i++) {
            //it simply construct the operations and return.
            transactionManager.Asy_ReadRecord(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
        }
    }


    /**
     * @param enqueue_time
     * @param index_time
     * @param keys
     * @param bid
     * @throws DatabaseException
     */
    private void write_requests(double[] enqueue_time, double[] index_time, int[] keys, List<DataBox>[] values, long bid) throws DatabaseException {

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_WriteRecord(txn_context, "MicroTable", String.valueOf(keys[i]), values[i], enqueue_time);//asynchronously return.
        }

    }

    private void read_core() throws InterruptedException {
        for (MicroEvent event : readEventHolder) {
            read_core(event);
        }
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        String componentId = context.getThisComponentId();

        if (in.isMarker()) {

            long bid = in.getBID();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid, bid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            read_core();

            END_COMPUTE_TIME_MEASURE(thread_Id);


            readEventHolder.clear();//all tuples in the readEventHolder is finished.

            if (enable_profile)
                writeEvents = 0;//all tuples in the holder is finished.

            final Marker marker = in.getMarker();

            this.collector.ack(in, marker);//tell spout it has finished the work.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);
        } else {

            long bid = in.getBID();
            String streamId = in.getSourceStreamId();
            boolean flag = next_decision();
            long timestamp;
            if (enable_latency_measurement)
                timestamp = in.getLong(0);
            else
                timestamp = 0L;//

            if (flag) {
                read_handle(bid, timestamp);
            } else {
                write_handle(bid, timestamp);
            }
        }
    }
}
