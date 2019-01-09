package brisk.components.operators.api;

import applications.param.*;
import applications.tools.FastZipfGenerator;
import applications.util.OsUtils;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import engine.profiler.Metrics;
import engine.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static applications.topology.transactional.State.partioned_store;
import static applications.topology.transactional.State.shared_store;

public abstract class TransactionalBolt<T> extends MapBolt implements Checkpointable {
    protected static final Logger LOG = LoggerFactory.getLogger(TransactionalBolt.class);
    private static final long serialVersionUID = -3899457584889441657L;

    protected TxnManager transactionManager;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    //    int interval;

//    private transient FastZipfGenerator shared_store;
//    private transient FastZipfGenerator[] partioned_store;


    private int i = 0;
    private int NUM_ITEMS;


    public TransactionalBolt(Logger log, int fid) {
        super(log);
        this.fid = fid;
        OsUtils.configLOG(LOG);
    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, long[] bid_array, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid]);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_UNLOCK(int _pid, int num_P, TxnManager txnManager, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).advance();
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }




    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        OsUtils.configLOG(LOG);
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        NUM_ACCESSES = Metrics.NUM_ACCESSES;
        //LOG.DEBUG("NUM_ACCESSES: " + NUM_ACCESSES + " theta:" + theta);
    }

    protected PKEvent generatePKEvent(long bid, Set<Integer> deviceID, double[][] value) {
        return new PKEvent(bid, deviceID, value);
    }




    public void dummayCalculation() {

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

}
