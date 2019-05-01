package applications.bolts.pk;


import applications.param.PKEvent;
import applications.parser.SensorParser;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.ordered.TxnManagerSStore;
import engine.transaction.impl.TxnContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.Set;

import static applications.constants.PositionKeepingConstants.Constant.SIZE_EVENT;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_VALUE;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;
import static utils.PartitionHelper.key_to_partition;

public class PKBolt_sstore extends PKBolt {


    private static final Logger LOG = LoggerFactory.getLogger(PKBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;

    final SensorParser parser = new SensorParser();
    private final ArrayDeque<PKEvent> PKEvents = new ArrayDeque<>();
    Random r = new Random();
    private double[][] value;

    public PKBolt_sstore(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        OsUtils.configLOG(LOG);

        value = new double[SIZE_EVENT][];
        for (int i = 0; i < SIZE_EVENT; i++) {
            value[i] = new double[SIZE_VALUE];
            for (int j = 0; j < SIZE_VALUE; j++) {
                value[i][j] = r.nextDouble() * 100;
            }
        }

    }

    private void event_handle(long[] bid, Set<Integer> deviceID) throws DatabaseException, InterruptedException {

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid, -1);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        PKEvent event = generatePKEvent(-1, deviceID, value);
        END_PREPARE_TIME_MEASURE(thread_Id);


        BEGIN_WAIT_TIME_MEASURE(thread_Id);

        for (Integer key : event.getKey()) {
            int p = key_to_partition(key);
            transactionManager.getOrderLock(p).blocking_wait(bid[p]);//ensures that locks are added in the event sequence order.
        }

        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        PK_request_lock_ahead(event, this.fid);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);

        for (Integer key : event.getKey()) {
            int p = key_to_partition(key);
            transactionManager.getOrderLock(p).advance();//ensures that locks are added in the event sequence order.
        }

        END_WAIT_TIME_MEASURE_ACC(thread_Id);

        BEGIN_TP_TIME_MEASURE(thread_Id);
        PK_request(event, this.fid);
        END_TP_TIME_MEASURE(thread_Id);

        BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
        PK_core(event);
        END_COMPUTE_TIME_MEASURE(thread_Id);

        transactionManager.CommitTransaction(txn_context);//always success..

        END_TRANSACTION_TIME_MEASURE(thread_Id, txn_context);

    }


    /**
     * @param event
     * @throws DatabaseException
     */
    private boolean PK_request_lock_ahead(@NotNull PKEvent event, int fid) throws DatabaseException {
        boolean flag = true;
        int i = 0;
        for (Integer key : event.getKey()) {
            flag &= transactionManager.lock_ahead(txn_context, "machine", String.valueOf(key), event.getList_value_ref(i++), READ_WRITE);// read the list value_list, and return.
        }
        return flag;
    }

    /**
     * @param event
     * @throws DatabaseException
     */
    private boolean PK_request(@NotNull PKEvent event, int fid) throws DatabaseException {
        boolean flag = true;
        int i = 0;
        for (Integer key : event.getKey())
            flag &= transactionManager.SelectKeyRecord_noLock(txn_context, "machine", String.valueOf(key), event.getList_value_ref(i++), READ_WRITE);// read the list value_list, and return.
        return flag;
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        String componentId = context.getThisComponentId();
        long[] partitionBID = in.getPartitionBID();
        Set<Integer> deviceID = (Set<Integer>) in.getValue(0);
        event_handle(partitionBID, deviceID);//calculate moving average.
    }

}
