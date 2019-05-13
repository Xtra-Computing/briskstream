package brisk.components.operators.api;

import applications.param.PKEvent;
import applications.sink.SINKCombo;
import applications.util.OsUtils;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import engine.DatabaseException;
import engine.profiler.Metrics;
import engine.transaction.TxnManager;
import engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SOURCE_CONTROL;

import java.util.Set;
import java.util.concurrent.BrokenBarrierException;

import static applications.CONTROL.enable_latency_measurement;
import static engine.profiler.Metrics.MeasureTools.*;

public abstract class TransactionalBolt<T> extends MapBolt implements Checkpointable {
    protected static final Logger LOG = LoggerFactory.getLogger(TransactionalBolt.class);
    private static final long serialVersionUID = -3899457584889441657L;

    public TxnManager transactionManager;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    protected int COMPUTE_COMPLEXITY;
    protected int POST_COMPUTE_COMPLEXITY;
    //    int interval;

//    private transient FastZipfGenerator shared_store;
//    private transient FastZipfGenerator[] partioned_store;


    private int i = 0;
    private int NUM_ITEMS;

    public SINKCombo sink = new SINKCombo();


    public TransactionalBolt(Logger log, int fid) {
        super(log);
        this.fid = fid;
        OsUtils.configLOG(LOG);
    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, long _bid, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(_bid, _bid);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }


    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, long[] bid_array, long _bid, int tthread) {

        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid], _bid);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_RESETALL(TxnManager txnManager, int tthread) {
        for (int k = 0; k < tthread; k++) {
            txnManager.getOrderLock(k).reset();
        }
    }

    public static void LA_UNLOCKALL(TxnManager txnManager, int tthread) {
        for (int k = 0; k < tthread; k++) {
            txnManager.getOrderLock(k).advance();
        }
    }

    public static void LA_UNLOCK(int _pid, int num_P, TxnManager txnManager, long _bid, int tthread) {

        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).advance();
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }


    protected PKEvent generatePKEvent(long bid, Set<Integer> deviceID, double[][] value) {
        return new PKEvent(bid, deviceID, value);
    }


    public void dummayCalculation() {

    }

    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {
        this.collector.broadcast_marker(streamId, bid, marker);//bolt needs to broadcast_marker
    }

    @Override
    public void ack_checkpoint(Marker marker) {
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }


    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        //pre stream processing phase..

        PRE_EXECUTE(in);

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        LAL_PROCESS(_bid);

        PostLAL_process(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.

        END_TOTAL_TIME_MEASURE_ACC(thread_Id, 1);//otherwise deadlock.
    }

    protected long timestamp;
    protected long _bid;
    protected Object input_event;


    int sum = 0;
//
//    private int dummy_compute() {
//
//        for (int j = 0; j < COMPUTE_COMPLEXITY; ++j)
//            sum += System.nanoTime();
//        return sum;
//    }

    protected void PRE_EXECUTE(Tuple in) {

        BEGIN_PREPARE_TIME_MEASURE(thread_Id);

        if (enable_latency_measurement)
            timestamp = in.getLong(1);
        else
            timestamp = 0L;//

        _bid = in.getBID();

        input_event = in.getValue(0);

//        int rt = 0;
//        if (enable_pre_compute)
//            rt = dummy_compute();

        txn_context[0] = new TxnContext(thread_Id, this.fid, this.i);

        sum = 0;

        END_PREPARE_TIME_MEASURE(thread_Id);
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..

        PRE_EXECUTE(in);

        PRE_TXN_PROCESS(_bid, timestamp);
    }

    protected long PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        return -1;
    }//only used by TSTREAM.


    protected void PostLAL_process(long bid) throws DatabaseException, InterruptedException {
    }

    protected void LAL_PROCESS(long bid) throws DatabaseException, InterruptedException {
    }

    protected void POST_PROCESS(long bid, long timestamp, int i) throws InterruptedException {

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        OsUtils.configLOG(LOG);
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        NUM_ACCESSES = Metrics.NUM_ACCESSES;
        COMPUTE_COMPLEXITY = Metrics.COMPUTE_COMPLEXITY;
        POST_COMPUTE_COMPLEXITY = Metrics.POST_COMPUTE_COMPLEXITY;
        //LOG.DEBUG("NUM_ACCESSES: " + NUM_ACCESSES + " theta:" + theta);

        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        SOURCE_CONTROL.getInstance().config(tthread);
    }

}
