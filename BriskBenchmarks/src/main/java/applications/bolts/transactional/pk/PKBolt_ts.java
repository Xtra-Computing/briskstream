package applications.bolts.transactional.pk;


import applications.param.PKEvent;
import applications.parser.SensorParser;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.transaction.dedicated.ordered.TxnManagerTStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;

import static applications.constants.PositionKeepingConstants.Constant.*;
import static engine.profiler.MeasureTools.*;

public class PKBolt_ts extends PKBolt {


    private static final Logger LOG = LoggerFactory.getLogger(PKBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;

    final SensorParser parser = new SensorParser();
    private final ArrayDeque<PKEvent> PKEvents = new ArrayDeque<>();
    Random r = new Random();
    private double[][] value;

    public PKBolt_ts(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_MACHINES, this.context.getThisComponent().getNumTasks());
        OsUtils.configLOG(LOG);

        value = new double[SIZE_EVENT][];
        for (int i = 0; i < SIZE_EVENT; i++) {
            value[i] = new double[SIZE_VALUE];
            for (int j = 0; j < SIZE_VALUE; j++) {
                value[i][j] = r.nextDouble() * 100;
            }
        }
    }

//
//    private void event_handle(long bid, Set<Integer> deviceID) throws DatabaseException {
//        BEGIN_WRITE_HANDLE_TIME_MEASURE(thread_Id);
//
//        BEGIN_MEASURE(thread_Id);
//        PKEvent input_event = generatePKEvent(bid, deviceID, value);
//        END_PREPARE_TIME_MEASURE(thread_Id);
//
//        txn_context = new TxnContext(thread_Id, this.fid, bid, input_event.index_ratio);//create a new txn_context for this new transaction.
//
//        PK_request(input_event, this.fid, bid);
//
//        PKEvents.add(input_event);
//
//        END_WRITE_HANDLE_TIME_MEASURE_TS(thread_Id);
//
//    }
//
//    /**
//     * @param input_event
//     * @param bid
//     * @throws DatabaseException
//     */
//    private void PK_request(PKEvent input_event, int fid, long bid) throws DatabaseException {
//
//        int i = 0;
//        for (Integer key : input_event.getKey()) {
//            transactionManager.Asy_ModifyRecord_Read(txn_context, "machine", String.valueOf(key),
//                    input_event.getMean_value_ref(i), new Mean(input_event.getValue(i)));// read and modify the mean value_list, and return.
//            i++;
//        }
//
//    }

    @Override
    public void execute(Tuple in) throws InterruptedException, BrokenBarrierException {
        String componentId = context.getThisComponentId();
        long bid = in.getBID();
        if (in.isMarker()) {

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.
            END_TP_TIME_MEASURE(thread_Id);

            //LOG.DEBUG("Task:" + thread_Id + " start to evaluate @" + DateTime.now());
            //Perform computation on each input_event and emit.


            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            //Spike detection.
            for (PKEvent event : PKEvents) {
                for (int i = 0; i < SIZE_EVENT; i++) {
                    double movingAverageInstant = event.getMean_value_ref(i).getRecord().getValue().getDouble();//getMean_value_ref null error.
                    double[] nextDouble = event.getValue(i);
                    boolean spike = Math.abs(nextDouble[SIZE_VALUE - 1] - movingAverageInstant) > SpikeThreshold * movingAverageInstant;
                    // measure_end the preconditions
                    collector.force_emit(bid, spike);
                }
            }

            END_ACCESS_TIME_MEASURE_ACC(thread_Id);


            final Marker marker = in.getMarker();
            this.collector.ack(in, marker);//tell spout it has finished the work.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id, 0);

            END_TOTAL_TIME_MEASURE_TS(thread_Id, PKEvents.size());

            PKEvents.clear();//all tuples in the holder is finished.
        } else {
            Set<Integer> deviceID = (Set<Integer>) in.getValue(0);
//            event_handle(bid, deviceID);//calculate moving average.
        }
    }
}
