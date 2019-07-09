package applications.bolts.pk;


import applications.param.txn.PKEvent;
import applications.parser.SensorParser;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerOrderLockBlocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.Set;

import static applications.constants.PositionKeepingConstants.Constant.SIZE_EVENT;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_VALUE;

public class PKBolt_olb extends PKBolt {


    private static final Logger LOG = LoggerFactory.getLogger(PKBolt_olb.class);
    private static final long serialVersionUID = -5968750340131744744L;

    final SensorParser parser = new SensorParser();
    private final ArrayDeque<PKEvent> PKEvents = new ArrayDeque<>();
    Random r = new Random();
    private double write_useful_time = 1000;//need to be measured before hand.
    private double[][] value;

    public PKBolt_olb(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerOrderLockBlocking(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
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
//    private void event_handle(long bid, Set<Integer> deviceID) throws DatabaseException, InterruptedException {
//
//        //begin transaction processing.
//        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
//        txn_context = new TxnContext(thread_Id, this.fid, bid);
//
//        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
//        PKEvent input_event = generatePKEvent(bid, deviceID, value);
//        END_PREPARE_TIME_MEASURE(thread_Id);
//
//        BEGIN_WAIT_TIME_MEASURE(thread_Id);
//        transactionManager.getOrderLock().blocking_wait(bid);//ensures that locks are added in the input_event sequence order.
//
//
//        BEGIN_LOCK_TIME_MEASURE(thread_Id);
//        PK_request_lock_ahead(input_event, this.fid, bid);
//        long lock_time_measure = END_LOCK_TIME_MEASURE_ACC(thread_Id);
//
//
//        transactionManager.getOrderLock().advance();//ensures that locks are added in the input_event sequence order.
//
//
//        END_WAIT_TIME_MEASURE_ACC(thread_Id, lock_time_measure);
//
//
//        BEGIN_TP_TIME_MEASURE(thread_Id);
//        PK_request(input_event, this.fid, bid);
//        END_TP_TIME_MEASURE(thread_Id);
//
//        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
//
//        PK_core(input_event);
//
//        END_COMPUTE_TIME_MEASURE(thread_Id);
//
//        transactionManager.CommitTransaction(txn_context);//always success..
//
//        END_TRANSACTION_TIME_MEASURE(thread_Id);
//
//    }
//
//
//    /**
//     * @param input_event
//     * @param bid
//     * @throws DatabaseException
//     */
//    private boolean PK_request_lock_ahead(@NotNull PKEvent input_event, int fid, long bid) throws DatabaseException {
//        boolean flag = true;
//        int i = 0;
//        for (Integer key : input_event.getKey()) {
//            flag &= transactionManager.lock_ahead(txn_context, "machine", String.valueOf(key), input_event.getList_value_ref(i++), READ_WRITE);// read the list value_list, and return.
//        }
//        return flag;
//    }
//
//    /**
//     * @param input_event
//     * @param bid
//     * @throws DatabaseException
//     */
//    private boolean PK_request(@NotNull PKEvent input_event, int fid, long bid) throws DatabaseException {
//        boolean flag = true;
//        int i = 0;
//        for (Integer key : input_event.getKey())
//            flag &= transactionManager.SelectKeyRecord_noLock(txn_context, "machine", String.valueOf(key), input_event.getList_value_ref(i++), READ_WRITE);// read the list value_list, and return.
//        return flag;
//    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        String componentId = context.getThisComponentId();
        long bid = in.getBID();
        Set<Integer> deviceID = (Set<Integer>) in.getValue(0);
//        event_handle(bid, deviceID);//calculate moving average.
    }
}
