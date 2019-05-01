package applications.bolts.pk;


import applications.param.PKEvent;
import applications.parser.SensorParser;
import applications.util.OsUtils;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import engine.DatabaseException;
import engine.transaction.dedicated.TxnManagerLock;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Random;
import java.util.Set;

import static applications.constants.PositionKeepingConstants.Constant.SIZE_EVENT;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_VALUE;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;
import static engine.profiler.Metrics.MeasureTools.*;

public class PKBolt_nocc extends PKBolt {


    private static final Logger LOG = LoggerFactory.getLogger(PKBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    final SensorParser parser = new SensorParser();
    private final ArrayDeque<PKEvent> PKEvents = new ArrayDeque<>();
    Random r = new Random();
    private double[][] value;
    public PKBolt_nocc(int fid) {
        super(LOG, fid);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        OsUtils.configLOG(LOG);
        value = new double[SIZE_EVENT][];
        for (int i = 0; i < SIZE_EVENT; i++) {
            value[i] = new double[SIZE_VALUE];
            for (int j = 0; j < SIZE_VALUE; j++) {
                value[i][j] = r.nextDouble() * 100;
            }
        }
    }

    private void event_handle(long bid, Set<Integer> deviceID) throws DatabaseException, InterruptedException {
        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);
        txn_context = new TxnContext(thread_Id, this.fid, bid);

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);
        PKEvent event = generatePKEvent(bid, deviceID, value);
        END_PREPARE_TIME_MEASURE(thread_Id);


        if (PK_request(event, this.fid, bid)) {

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            PK_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            CLEAN_ABORT_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);

        } else {

            BEGIN_ABORT_TIME_MEASURE(thread_Id);
            while (!PK_request(event, this.fid, bid)) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
            END_ABORT_TIME_MEASURE_ACC(thread_Id);

            BEGIN_COMPUTE_TIME_MEASURE(thread_Id);
            PK_core(event);
            END_COMPUTE_TIME_MEASURE(thread_Id);

            transactionManager.CommitTransaction(txn_context);//always success..
            END_TRANSACTION_TIME_MEASURE(thread_Id);
        }


    }

    /**
     * @param event
     * @param bid
     * @throws DatabaseException
     */
    private boolean PK_request(PKEvent event, int fid, long bid) throws DatabaseException, InterruptedException {

        int i = 0;
        for (Integer key : event.getKey()) {
            boolean rt = transactionManager.SelectKeyRecord(txn_context, "machine", String.valueOf(key), event.getList_value_ref(i), READ_WRITE);// read the list value_list, and return.
            if (rt) {
                assert event.getList_value_ref(i).getRecord() != null;
            } else {
                return false;
            }
            i++;
        }

        return true;

    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        String componentId = context.getThisComponentId();
        long bid = in.getBID();
        Set<Integer> deviceID = (Set<Integer>) in.getValue(0);
        event_handle(bid, deviceID);//calculate moving average.
    }
}
