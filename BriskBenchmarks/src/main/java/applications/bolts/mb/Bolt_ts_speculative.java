package applications.bolts.mb;


import applications.CONTROL;
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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.profiler.Metrics.MeasureTools.*;

public class Bolt_ts_speculative extends MBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Bolt_ts_speculative.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //    LinkedList<Long> gap = new LinkedList<>();
    private final double write_useful_time = 1477.0;//pre-measured.
    double queue_time = 0;//time spend in enqueueing all events in current epoch.
    double index_time = 0;
    int writeEvents = 0;
    private List<MicroEvent> readEventHolder = new LinkedList<>();

    public Bolt_ts_speculative(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    private void read_handle(long bid, Long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_READ_HANDLE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        read_requests_speculative(event, this.fid, bid);

        //Earlier returns.
        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        long start = System.nanoTime();
        int sum = 0;
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            DataBox dataBox = ref.record.getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            sum += read_result;

        }

        collector.force_emit(event.getBid(), sum);//the tuple is finished.

        read_requests(event, this.fid, bid);
        readEventHolder.add(event);//mark the tuple as ``in-complete"

        END_COMPUTE_TIME_MEASURE_TS(thread_Id, write_useful_time, writeEvents);
    }

    private void write_handle(long bid, Long timestamp) throws InterruptedException, DatabaseException {

        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);
        MicroEvent event = next_event(bid, timestamp);
        write_requests(event.enqueue_time, event.index_time, event.getKeys(), event.getValues(), event.getBid());

        END_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        if (CONTROL.enable_profile)
            writeEvents++;//just for record purpose.

        collector.force_emit(event.getBid(), event);//the tuple is immediately finished.
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


//    private List<MicroEvent> writeEventHolder = new LinkedList<>();

    /**
     * @param event
     * @param fid
     * @param bid
     * @throws DatabaseException
     */
    private void read_requests(MicroEvent event, int fid, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_ReadRecord(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
        }
    }

    /**
     * Speculatively read records without waiting for others to be ready.
     *
     * @param event
     * @param fid
     * @param bid
     * @throws DatabaseException
     */
    private void read_requests_speculative(MicroEvent event, int fid, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid, event.index_time);//create a new txn_context for this new transaction.

        for (int i = 0; i < NUM_ACCESSES; ++i) {
            transactionManager.SelectKeyRecord_noLock(txn_context, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_ONLY);
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

        txn_context = new TxnContext(thread_Id, this.fid, bid, index_time);//create a new txn_context for this new transaction.
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_WriteRecord(txn_context, "MicroTable", String.valueOf(keys[i]), values[i], enqueue_time);//asynchronously return.
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        long bid = in.getBID();
        String componentId = context.getThisComponentId();


        if (in.isMarker()) {
            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(this.context.getThisTaskId(), this.fid, bid);//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

            //Apologize if there is any false reading..
            //omit the process of apologize.

            readEventHolder.clear();//all tuples in the readEventHolder is finished.
            if (CONTROL.enable_profile)
                writeEvents = 0;//all tuples in the holder is finished.

            final Marker marker = in.getMarker();
            this.collector.ack(in, marker);//tell spout it has finished the work.
            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);
        } else {
            String streamId = in.getSourceStreamId();
            boolean flag = in.getBoolean(0);
            Long timestamp = in.getLong(1);
            if (flag) {
                read_handle(bid, timestamp);
            } else {
                write_handle(bid, timestamp);
            }
        }


    }
}
