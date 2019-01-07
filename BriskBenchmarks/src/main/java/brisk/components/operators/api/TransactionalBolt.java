package brisk.components.operators.api;

import applications.param.*;
import applications.tools.FastZipfGenerator;
import applications.util.OsUtils;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.impl.Marker;
import engine.common.PartitionedOrderLock;
import engine.profiler.Metrics;
import engine.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;

import static applications.constants.CrossTableConstants.Constant.*;
import static applications.topology.transactional.State.partioned_store;
import static applications.topology.transactional.State.shared_store;

public abstract class TransactionalBolt<T> extends MapBolt implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalBolt.class);
    private static final long serialVersionUID = -3899457584889441657L;

    protected TxnManager transactionManager;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    //    int interval;

//    private transient FastZipfGenerator shared_store;
//    private transient FastZipfGenerator[] partioned_store;

    //different R-W ratio.
    //just enable one of the decision array
    protected transient boolean[] read_decision;
    private int i = 0;
    private int NUM_ITEMS;


    public TransactionalBolt(Logger log, int fid) {
        super(log);
        this.fid = fid;
        OsUtils.configLOG(LOG);
    }

    public static void LA_LOCK(int _pid, int i, PartitionedOrderLock.LOCK orderLock, long[] bid_array, boolean b) {
        for (int k = 0; k < i; k++) {
            orderLock.blocking_wait(bid_array[_pid]);
            _pid++;
            if (b)
                _pid = 0;
        }
    }

    public static void LA_UNLOCK(int _pid, int i, PartitionedOrderLock.LOCK orderLock, boolean b) {
        for (int k = 0; k < i; k++) {
            orderLock.advance();
            _pid++;
            if (b)
                _pid = 0;
        }
    }

    protected boolean next_decision() {

        boolean rt = read_decision[i];

        i++;
        if (i == 8)
            i = 0;
        return rt;

    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        OsUtils.configLOG(LOG);
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);

        NUM_ACCESSES = Metrics.NUM_ACCESSES;
        //LOG.DEBUG("NUM_ACCESSES: " + NUM_ACCESSES + " theta:" + theta);


        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

        if (ratio_of_read == 0) {
            read_decision = new boolean[]{false, false, false, false, false, false, false, false};// all write.
        } else if (ratio_of_read == 0.25) {
            read_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% W, 25% R.
        } else if (ratio_of_read == 0.5) {
            read_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal r-w ratio.
        } else if (ratio_of_read == 0.75) {
            read_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% W, 75% R.
        } else if (ratio_of_read == 1) {
            read_decision = new boolean[]{true, true, true, true, true, true, true, true};// all read.
        } else {
            throw new UnsupportedOperationException();
        }



        LOG.info("ratio_of_read: " + ratio_of_read + "\tREAD DECISIONS: " + Arrays.toString(read_decision));
    }


    protected DepositEvent randomDepositEvent(int pid, int number_of_partitions, long bid, long timestamp, SplittableRandom rnd) {


//        int partition_offset = pid * floor_interval;
//        int account_range = floor_interval- 1;
//        int asset_range = floor_interval - 1;

//        FastZipfGenerator generator = partioned_store[pid];

        int _pid = pid;

        //key
        final int account = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

        _pid++;
        if (_pid == tthread)
            _pid = 0;

        final int book = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;


        //value_list
        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit = rnd.nextLong(MAX_BOOK_TRANSFER);

        return new DepositEvent(
                bid, ACCOUNT_ID_PREFIX + account,
                BOOK_ENTRY_ID_PREFIX + book,
                accountsDeposit,
                deposit);
    }

    /**
     * Used in CT.
     *
     * @param bid
     * @param rnd
     * @return
     */
    protected DepositEvent randomDepositEvent(long bid, SplittableRandom rnd) {
//        final int account = rnd.nextInt(NUM_ACCOUNTS - 1);
//        final int book = rnd.nextInt(NUM_BOOK_ENTRIES - 1);
        FastZipfGenerator generator = shared_store;


        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit = rnd.nextLong(MAX_BOOK_TRANSFER);

        return new DepositEvent(
                bid, ACCOUNT_ID_PREFIX + generator.next(),
                BOOK_ENTRY_ID_PREFIX + generator.next(),
                accountsDeposit,
                deposit);
    }

    protected TransactionEvent randomTransactionEvent(int pid, int number_of_partitions, long bid, long timestamp, SplittableRandom rnd) {


        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);

//        int partition_offset = pid * floor_interval;
//        int account_range = floor_interval - 1;
//        int asset_range = floor_interval - 1;

//        FastZipfGenerator generator = partioned_store[pid];


        while (!Thread.currentThread().isInterrupted()) {
            int _pid = pid;

            final int sourceAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

            _pid++;
            if (_pid == tthread)
                _pid = 0;


            final int targetAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;

            _pid++;
            if (_pid == tthread)
                _pid = 0;


            final int sourceBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;

            _pid++;
            if (_pid == tthread)
                _pid = 0;


            final int targetBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;

            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            return new TransactionEvent(
                    bid, ACCOUNT_ID_PREFIX + sourceAcct,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }

        return null;
    }


    protected TransactionEvent randomTransactionEvent(long bid, SplittableRandom rnd) {
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);

        FastZipfGenerator generator = shared_store;
        while (!Thread.currentThread().isInterrupted()) {
            final int sourceAcct = generator.next();//rnd.nextInt(NUM_ACCOUNTS - 1);
            final int targetAcct = generator.next();//rnd.nextInt(NUM_ACCOUNTS - 1);
            final int sourceBook = generator.next();//rnd.nextInt(NUM_BOOK_ENTRIES - 1);
            final int targetBook = generator.next();//rnd.nextInt(NUM_BOOK_ENTRIES - 1);

            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            return new TransactionEvent(
                    bid, ACCOUNT_ID_PREFIX + sourceAcct,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }
        return null;
    }


    protected PKEvent generatePKEvent(long bid, Set<Integer> deviceID, double[][] value) {

        return new PKEvent(bid, deviceID, value);
    }

    protected abstract Object next_event(long bid, Long timestamp);

    /**
     * Every event has its corresponding working set.
     * Used in MB.
     */
    protected MicroEvent generateEvent() {
        MicroParam param = new MicroParam(NUM_ACCESSES);

        Set keys = new HashSet();
        for (int access_id = 0; access_id < NUM_ACCESSES; ++access_id) {
            int res = shared_store.next();
            //should not have duplicate keys.
//            while (keys.contains(res)) {
//                res = shared_store.next();
//            }
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS)
//                    res = 0;
                res = shared_store.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
        }
        return new MicroEvent(true, param.keys(), NUM_ACCESSES);
    }

    /**
     * Generate events according to the given parition_id.
     *
     * @param partition_id
     * @return
     */
    protected MicroEvent generateEvent(int partition_id, int number_of_partitions) {

        int pid = partition_id;
        MicroParam param = new MicroParam(NUM_ACCESSES);

        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(NUM_ACCESSES / (double) number_of_partitions);

        int counter = 0;

        for (int access_id = 0; access_id < NUM_ACCESSES; ++access_id) {
            FastZipfGenerator generator = partioned_store[pid];
            int res = generator.next();
            //should not have duplicate keys.
            while (keys.contains(res) && !Thread.currentThread().isInterrupted()) {
//                res++;//speed up the search for non-duplicate key.
//                if (res == NUM_ITEMS) {
//                    res = partition_id * interval;
//                }
                res = generator.next();
            }

            keys.add(res);
            param.set_keys(access_id, res);
            counter++;
            if (counter == access_per_partition) {
//                pointer++;
                pid++;
                if (pid == tthread)
                    pid = 0;

                counter = 0;
            }
        }

//        assert verify(keys, partition_id, number_of_partitions);

        return new MicroEvent(true, param.keys(), NUM_ACCESSES);

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
