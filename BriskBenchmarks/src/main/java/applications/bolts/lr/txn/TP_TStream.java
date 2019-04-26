package applications.bolts.lr.txn;


import applications.datatype.PositionReport;
import applications.datatype.internal.AvgVehicleSpeedTuple;
import applications.param.lr.LREvent;
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

import static applications.CONTROL.enable_debug;
import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for TStream.
 */
public class TP_TStream extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TP_TStream.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private ArrayDeque<LREvent> LREvents = new ArrayDeque<>();

    public TP_TStream(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getThisTaskId(), context.getGraph());
    }

    @Override
    protected void read_handle(LREvent event, Long timestamp) throws DatabaseException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        read_request(event, event.getBid());

        LREvents.add(event);

        END_WRITE_HANDLE_TIME_MEASURE(thread_Id);

    }


    @Override
    protected void write_handle(LREvent event, Long timestamp) throws DatabaseException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        write_request(event, event.getBid());

        LREvents.add(event);

        END_WRITE_HANDLE_TIME_MEASURE(thread_Id);

    }

    private void read_request(LREvent event, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid);
        transactionManager.Asy_ReadRecord(txn_context
                , "segment_speed"
                , String.valueOf(event.getVSreport().getSegment())
                , event.speed_value
                , event.enqueue_time);

        transactionManager.Asy_ReadRecord(txn_context
                , "segment_cnt"
                , String.valueOf(event.getVSreport().getSegment())
                , event.count_value
                , event.enqueue_time);
    }

    protected void write_request(LREvent event, long bid) throws DatabaseException {
        txn_context = new TxnContext(thread_Id, this.fid, bid);//create a new txn_context for this new transaction.
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord_Read(txn_context
                , "segment_speed"
                , String.valueOf(event.getVSreport().getSegment())
                , event.speed_value//holder to be filled up.
                , new AVG(event.getVSreport().getAvgSpeed())
        );          //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txn_context
                , "segment_cnt"
                , String.valueOf(event.getVSreport().getSegment())
                , event.count_value//holder to be filled up.
                , new CNT(event.getPOSReport().getVid())
        );          //asynchronously return.

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(config, db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);

            for (LREvent event : LREvents) {
                post_process(event);
            }

            END_COMPUTE_TIME_MEASURE(thread_Id);

            LREvents.clear();//clear stored events.

            final Marker marker = in.getMarker();

            this.collector.ack(in, marker);//tell spout it has finished the work.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id);

        } else {

            BEGIN_PREPARE_TIME_MEASURE(thread_Id);

            long bid = in.getBID();

            Long timestamp;//in.getLong(1);

            if (enable_latency_measurement)
                timestamp = in.getLong(0);
            else
                timestamp = 0L;//

            //pre process.
//            String[] token = parser(in);
//            PositionReport report = dispatcher(token);

            PositionReport report = (PositionReport) in.getValue(0);
            int vid = report.getVid();
            int speed = report.getSpeed().intValue();
            this.segment.set(report);

            AvgVehicleSpeedTuple vehicleSpeedTuple = update_avgsv(vid, speed);

            LREvent event = new LREvent(report, vehicleSpeedTuple, bid);
            //txn process.

            //update segment statistics. Write and Read Requests.
            write_handle(event, timestamp);

            //post process.

            if (enable_debug)
                LOG.info("Commit event of bid:" + bid);

        }
    }
}
