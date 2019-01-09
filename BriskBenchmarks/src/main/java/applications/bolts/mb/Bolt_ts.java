package applications.bolts.mb;


import applications.param.MicroEvent;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.storage.SchemaRecordRef;
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

    @Override
    protected void read_handle(MicroEvent event, Long timestamp) throws DatabaseException, InterruptedException {

        BEGIN_READ_HANDLE_TIME_MEASURE(thread_Id);
        long bid = event.getBid();
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());

        read_requests(event, this.fid, bid);

        readEventHolder.add(event);//mark the tuple as ``in-complete"
        END_READ_HANDLE_TIME_MEASURE(thread_Id);

        if (enable_speculative) {
            //earlier emit
            collector.force_emit(event.getBid(), 1, event.getTimestamp());//the tuple is finished.
        }
    }

    @Override
    protected void write_handle(MicroEvent event, Long timestamp) throws InterruptedException, DatabaseException {

        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);
        long bid = event.getBid();
        txn_context = new TxnContext(thread_Id, this.fid, event.getBid());
        write_requests(event.enqueue_time, event.index_time, event.getKeys(), event.getValues(), bid);

        if (enable_profile)
            writeEvents++;//just for record purpose.

        collector.force_emit(bid, true, event.getTimestamp());//the tuple is immediately finished.

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
            SchemaRecordRef ref = event.getRecord_refs()[i];
            assert ref.cnt == 0;
//            LOG.info("Insert ref:" + OsUtils.Addresser.addressOf(ref));
            transactionManager.Asy_ReadRecord(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), ref, event.enqueue_time);
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

            BEGIN_PREPARE_TIME_MEASURE(thread_Id);
            long bid = in.getBID();


            MicroEvent event = (MicroEvent) db.eventManager.get((int) bid);

            long timestamp;
            if (enable_latency_measurement)
                timestamp = in.getLong(0);
            else
                timestamp = 0L;//

            boolean flag = event.READ_EVENT();
            (event).setTimestamp(timestamp);
            END_PREPARE_TIME_MEASURE_TS(thread_Id);

            if (flag) {
                read_handle(event, timestamp);
            } else {
                write_handle(event, timestamp);
            }
        }
    }
}
