package applications.bolts.lr.txn;


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

import static engine.profiler.Metrics.MeasureTools.*;


/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_TStream extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private ArrayDeque<LREvent> LREvents = new ArrayDeque<>();


    public TPBolt_TStream(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    public void loadData(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadData(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getThisTaskId(), context.getGraph());
    }


    @Override
    protected void write_handle(LREvent event, Long timestamp) throws DatabaseException {
        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);

        write_request(event, event.getBid());

        LREvents.add(event);

        END_WRITE_HANDLE_TIME_MEASURE_TS(thread_Id);

    }

    @Override
    protected void read_core(LREvent event) throws InterruptedException {
        Integer vid = event.getVSreport().getVid();
        int count = event.count_value.getRecord().getValue().getInt();
        double lav = event.speed_value.getRecord().getValue().getDouble();


        toll_process(event.getBid(), vid, count, lav, event.getPOSReport().getTime());
//        int sum = 0;
//        for (int i = 0; i < NUM_ACCESSES; ++i) {
//            SchemaRecordRef ref = event.getRecord_refs()[i];
//            try {
//                DataBox dataBox = ref.getRecord().getValues().get(1);
//                int read_result = Integer.parseInt(dataBox.getString().trim());
//                sum += read_result;
//            } catch (Exception e) {
//                System.out.println("Null Pointer Exception at: " + event.getBid() + "i:" + i);
//                System.out.println("What causes the exception?" + event.getKeys()[i]);
//            }
//        }
//
//        if (enable_speculative) {
//            //measure_end if the previous send sum is wrong. if yes, send a signal to correct it. otherwise don't send.
//            //now we assume it's all correct for testing its upper bond.
//            //so nothing is send out.
//        } else
//            collector.force_emit(event.getBid(), sum, event.getTimestamp());//the tuple is finished finally.
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
                read_core(event);
            }

            END_COMPUTE_TIME_MEASURE_TS(thread_Id, 0, LREvents.size(), 0);


            final Marker marker = in.getMarker();

            this.collector.ack(in, marker);//tell spout it has finished the work.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id, LREvents.size());

            LREvents.clear();//clear stored events.


        } else {
            super.execute(in);
        }
    }
}
