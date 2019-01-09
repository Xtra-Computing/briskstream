package engine.transaction.dedicated.ordered;

import applications.util.OsUtils;
import engine.common.Operation;
import engine.index.high_scale_lib.ConcurrentHashMap;
import engine.profiler.Metrics;
import engine.storage.SchemaRecord;
import engine.storage.datatype.DataBox;
import engine.storage.datatype.DoubleDataBox;
import engine.storage.datatype.ListDoubleDataBox;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import engine.transaction.function.Mean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static applications.CONTROL.*;
import static applications.constants.MicroBenchmarkConstants.Constant.VALUE_LEN;
import static applications.constants.PositionKeepingConstants.Constant.MOVING_AVERAGE_WINDOW;
import static applications.constants.PositionKeepingConstants.Constant.SIZE_VALUE;
import static engine.Meta.MetaTypes.AccessType.*;
import static engine.profiler.Metrics.MeasureTools.*;
import static xerial.jnuma.Numa.runOnNode;

/**
 * There is one TxnProcessingEngine of each stage.
 * This is closely bundled with the start_ready map.
 * <p>
 * It now only works for single stage..
 */
public final class TxnProcessingEngine {
    private static final Logger LOG = LoggerFactory.getLogger(TxnProcessingEngine.class);
    private static TxnProcessingEngine instance = new TxnProcessingEngine();
    Metrics metrics;

    private Integer num_op = -1;
    private Integer first_exe;
    private Integer last_exe;
    private CyclicBarrier barrier;
    private Instance standalone_engine;
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support.
    private int app;

    private TxnProcessingEngine() {
        OsUtils.configLOG(LOG);
    }

    //    fast determine the corresponding instance. This design is for NUMA-awareness.
    private HashMap<Integer, Instance> numa_engine = new HashMap<>();//one island one engine.
//    private int partition = 1;//NUMA-awareness. Hardware Island. If it is one, it is the default shared-everything.
//    private int range_min = 0;
//    private int range_max = 1_000_000;//change this for different application.

    /**
     * @return
     */
    public static TxnProcessingEngine getInstance() {
        return instance;
    }

    public void initilize(int size, int app) {
        num_op = size;
        this.app = app;
//        holder_by_stage = new Holder_in_range(num_op);
        holder_by_stage = new ConcurrentHashMap<>();


        //make it flexible later.
        if (app == 1)//CT
        {
            holder_by_stage.put("accounts", new Holder_in_range(num_op));
            holder_by_stage.put("bookEntries", new Holder_in_range(num_op));
        } else if (app == 2) {//OB
            holder_by_stage.put("goods", new Holder_in_range(num_op));
        } else {//MB
            holder_by_stage.put("MicroTable", new Holder_in_range(num_op));
        }

        metrics = Metrics.getInstance();
    }

    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }

    public void engine_init(Integer first_exe, Integer last_exe, Integer stage_size, int tp) {
        this.first_exe = first_exe;
        this.last_exe = last_exe;
        num_op = stage_size;
        barrier = new CyclicBarrier(stage_size);
        //start_ready = new ResettableCountDownLatch(num_op);
//		process_ready = new ResettableCountDownLatch(num_op);
//		process_barrier = new CyclicBarrier(num_op);

        if (enable_multi_engine) {
            for (int i = 0; i < island; i++) {
                numa_engine.put(i, new Instance(tp / island));
//                numa_engine.get(i).executor.submit(new dummyTask(i));
            }
        } else {
            //single box numa_engine.
            standalone_engine = new Instance(tp);

        }

        LOG.info("source state initialize");
    }


    public void engine_shutdown() {
        if (enable_multi_engine) {
            for (int i = 0; i < island; i++) {
                numa_engine.get(i).close();
            }
        } else {
            //single box numa_engine.
            standalone_engine.close();
        }
    }

    private void CT_Transfer_Fun(Operation operation) {

        // read
        final long sourceAccountBalance = operation.condition_records[0].content_.readPreValues(operation.bid).getValues().get(1).getLong();
        final long sourceAssetValue = operation.condition_records[1].content_.readPreValues(operation.bid).getValues().get(1).getLong();

        //when d_record is different from condition record
        //It may generate cross-records dependency problem.
        //Fix it later.

        // check the preconditions
        //TODO: make the condition checking more generic in future.
        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2
                && sourceAssetValue > operation.condition.arg3) {

            //read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            List<DataBox> values = srcRecord.getValues();

            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record

            //apply function.
            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta);//compute.
            } else
                throw new UnsupportedOperationException();

            operation.d_record.content_.WriteAccess(operation.bid, tempo_record);//it may reduce NUMA-traffic.
            //Operation.d_record.content_.WriteAccess(Operation.bid, new SchemaRecord(values), wid);//does this even needed?
            operation.success[0] = true;
//            if (operation.table_name.equalsIgnoreCase("accounts") && operation.d_record.record_.GetPrimaryKey().equalsIgnoreCase("11")) {
//            LOG.info("key: " + operation.d_record.record_.GetPrimaryKey() + " BID: " + operation.bid + " set " + operation.success.hashCode() + " to true." + " sourceAccountBalance:" + sourceAccountBalance);
//            }
        } else {

//            if (operation.success[0] == true)
//                System.nanoTime();
            operation.success[0] = false;
        }
    }

    private void CT_Depo_Fun(Operation operation) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(operation.column_id).incLong(operation.function.delta);//compute.
        operation.s_record.content_.WriteAccess(operation.bid, tempo_record);//it may reduce NUMA-traffic.
    }

    private void process(Operation operation) {
        if (operation.accessType == READ_ONLY) {
            if (enable_mvcc)
                operation.record_ref.record = operation.d_record.content_.ReadAccess(operation.bid, READ_ONLY);
            else
                operation.record_ref.record = operation.d_record.record_;
        } else if (operation.accessType == WRITE_ONLY) {//push evaluation down. --only used for MB.

            if (operation.value_list != null) { //directly replace value_list
                if (enable_mvcc)
                    operation.d_record.content_.WriteAccess(operation.bid, new SchemaRecord(operation.value_list));//it may reduce NUMA-traffic.
                else
                    operation.d_record.record_.getValues().get(operation.column_id).setString(operation.value_list.get(1).getString(), VALUE_LEN);
            } else { //update by column_id.
                operation.d_record.record_.getValues().get(operation.column_id).setLong(operation.value);
//                LOG.info("Alert price:" + operation.value);
            }
        } else if (operation.accessType == READ_WRITE) {//read, modify, write.

            if (app == 1) {
                CT_Depo_Fun(operation);//used in CT
            } else {
                SchemaRecord srcRecord = operation.s_record.content_.ReadAccess(operation.bid, operation.accessType);
                List<DataBox> values = srcRecord.getValues();

                //apply function to modify..
                if (operation.function instanceof INC) {
                    values.get(operation.column_id).setLong(values.get(operation.column_id).getLong() + operation.function.delta);
                } else
                    throw new UnsupportedOperationException();
            }

        } else if (operation.accessType == READ_WRITE_COND) {//read, modify (depends on condition), write( depends on condition).
            //TODO: pass function here in future instead of hard-code it. Seems not trivial in Java, consider callable interface?

            if (app == 1) {//used in CT
                CT_Transfer_Fun(operation);
            } else if (app == 2) {//used in OB
                //check if any item is not able to buy.
                List<DataBox> d_record = operation.condition_records[0].content_.ReadAccess(operation.bid, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = operation.condition.arg1;
                long bid_qty = operation.condition.arg2;

                // check the preconditions
                if (bidPrice < askPrice || bid_qty > left_qty) {
                    operation.success[0] = false;
                } else {
                    d_record.get(2).setLong(left_qty - operation.function.delta);//new quantity.
                    operation.success[0] = true;
                }
            }

        } else if (operation.accessType == READ_WRITE_COND_READ) {
            assert operation.record_ref != null;
            if (app == 1) {//used in CT
                CT_Transfer_Fun(operation);

                if (operation.success[0])
                    operation.record_ref.record = operation.d_record.content_.readValues(operation.bid);//read the resulting tuple.
                else
                    operation.record_ref.record = operation.d_record.content_.readValues(operation.bid);//read the resulting tuple.

//                if (operation.record_ref.record == null) {
//                    System.nanoTime();
//                }
            } else
                throw new UnsupportedOperationException();

        } else if (operation.accessType == READ_WRITE_READ) {//used in PK.
            assert operation.record_ref != null;

            //read source.
            List<DataBox> srcRecord = operation.s_record.content_.ReadAccess(operation.bid, operation.accessType).getValues();

            //apply function.
            if (operation.function instanceof Mean) {

                // compute.
                ListDoubleDataBox valueList = (ListDoubleDataBox) srcRecord.get(1);
                double sum = srcRecord.get(2).getDouble();
                double[] nextDouble = operation.function.new_value;

                for (int j = 0; j < SIZE_VALUE; j++) {
                    sum -= valueList.addItem(nextDouble[j]);
                    sum += nextDouble[j];
                }

                // update content.
                srcRecord.get(2).setDouble(sum);
                // Operation.d_record.content_.WriteAccess(Operation.bid, new SchemaRecord(srcRecord), wid);//not even needed.

                // configure return-record.
                if (valueList.size() < MOVING_AVERAGE_WINDOW) {//just added
                    operation.record_ref.record = new SchemaRecord(new DoubleDataBox(nextDouble[SIZE_VALUE - 1]));
                } else {
                    operation.record_ref.record = new SchemaRecord(new DoubleDataBox(sum / MOVING_AVERAGE_WINDOW));
                }
//                            LOG.info("BID:" + Operation.bid + " is set @" + DateTime.now());
            } else
                throw new UnsupportedOperationException();

        }
    }


    //TODO: actual evaluation on the operation_chain.
    private void process(MyList<Operation> operation_chain) {

//        if (enable_engine && enable_work_stealing) {
//            while (true) {
//                Operation operation = operation_chain.pollFirst();
//                if (operation == null) return;
//                process(operation);
//            }//loop.
//        }
//        else {
//            if (operation_chain.getTable_name().equalsIgnoreCase("accounts") && operation_chain.getPrimaryKey().equalsIgnoreCase("11")) {
//                System.nanoTime();
//            }
        for (Operation operation : operation_chain) {
            process(operation);
//                if (operation_chain.getTable_name().equalsIgnoreCase("accounts") && operation_chain.getPrimaryKey().equalsIgnoreCase("11"))
//                    LOG.info("finished process bid:" + operation.bid + " by " + Thread.currentThread().getName());
        }//loop.
//        }
    }


    private int ThreadToSocket(int thread_Id) {

        return (thread_Id + 2) % CORE_PER_SOCKET;
    }

    private int ThreadToEngine(int thread_Id) {

        int rt = (thread_Id + 2) / (TOTAL_CORES / island);
        // LOG.debug("submit to engine: "+ rt);
        return rt;
    }

    private int submit_task(int thread_Id, Holder holder, long bid, Deque<Task> callables) {

        int sum = 0;

//        Instance instance = standalone_engine;//numa_engine.get(key);
//        assert instance != null;
//        for (Map.Entry<Range<Integer>, Holder> rangeHolderEntry : holder.entrySet()) {
//            Holder operation_chain = rangeHolderEntry.getValue();

        for (MyList<Operation> operation_chain : holder.holder_v1.values()) {
//        for (int i = 0; i < H2_SIZE; i++) {
//            ConcurrentSkipListSet<Operation> operation_chain = holder.holder_v2[i];
//        Set<Operation> operation_chain = holder.holder_v3;
            if (operation_chain.size() > 0) {
                if (enable_debug)
                    sum += operation_chain.size();

//                Instance instance = standalone_engine;//numa_engine.get(key);
                if (!Thread.currentThread().isInterrupted()) {
                    if (enable_engine) {
                        Task task = new Task(operation_chain, bid);
//                        LOG.debug("Submit operation_chain:" + OsUtils.Addresser.addressOf(operation_chain) + " with size:" + operation_chain.size());

                        if (enable_multi_engine) {
                            numa_engine.get(ThreadToEngine(thread_Id)).executor.submit(task);
                        } else {
                            standalone_engine.executor.submit(task);
                        }
                        callables.add(task);
                    } else {
                        process(operation_chain);//directly apply the computation.
                    }
                }
            }
        }
        return sum;
    }

    private int evaluation(int thread_Id, long bid) throws InterruptedException {

        BEGIN_TP_SUBMIT_TIME_MEASURE(thread_Id);

        //LOG.DEBUG(thread_Id + "\tall source marked checkpoint, starts TP evaluation for watermark bid\t" + bid);

        Deque<Task> callables = new ArrayDeque<>();

        int task = 0;

        for (Holder_in_range holder_in_range : holder_by_stage.values()) {
            Holder holder = holder_in_range.rangeMap.get(thread_Id);
            task += submit_task(thread_Id, holder, bid, callables);
        }

        END_TP_SUBMIT_TIME_MEASURE(thread_Id, task);

        if (enable_engine) {
            if (enable_multi_engine) {
                numa_engine.get(ThreadToEngine(thread_Id)).executor.invokeAll(callables);
            } else
                standalone_engine.executor.invokeAll(callables);
        }

        //blocking wait for all operation_chain to complete.
        //TODO: For now, we don't know the relationship between operation_chain and transaction, otherwise, we can asynchronously return.
//        holder.holder_v1.clear();

        if (enable_debug)
            LOG.info("finished task:" + task);

        return task;
    }

    private void initilize(Holder holder) {
        holder.holder_v1 = new ConcurrentHashMap<>();
    }

    /**
     * @param thread_Id
     * @param bid
     * @return time spend in tp evaluation.
     * @throws InterruptedException
     * @throws BrokenBarrierException
     */
    public void start_evaluation(int thread_Id, long bid) throws InterruptedException, BrokenBarrierException {

        //It first needs to make sure checkpoints from all producers are received.
        barrier.await();

        BEGIN_TP_CORE_TIME_MEASURE(thread_Id);

        int size = evaluation(thread_Id, bid);

        END_TP_CORE_TIME_MEASURE(thread_Id, size);//exclude task submission time.

        barrier.await();// Because the insertor (operator) does not know if his stored event has been processed or not.
    }

    /**
     * There shall be $num_op$ Holders.
     */
    public class Holder {
        //version 1: single list Operation on one key
        //	ConcurrentSkipListSet holder_v1 = new ConcurrentSkipListSet();
        public ConcurrentHashMap<String, MyList<Operation>> holder_v1 = new ConcurrentHashMap<>();
//        public ConcurrentSkipListSet<Operation>[] holder_v2 = new ConcurrentSkipListSet[H2_SIZE];

    }

    public class Holder_in_range {
        public HashMap<Integer, Holder> rangeMap = new HashMap<>();//each op has a holder.

        public Holder_in_range(Integer num_op) {
            int i;
            for (i = 0; i < num_op; i++) {
                rangeMap.put(i, new Holder());
            }
        }
    }

    /**
     * TP-processing instance.
     * If it is one, it is a shared everything configuration.
     * Otherwise, we use hash partition to distribute the data and workload.
     */
    class Instance implements Closeable {
        public ExecutorService executor;
        int range_min;
        int range_max;
//        ExecutorCompletionService<Integer> TP_THREADS; // In the next work, we can try asynchronous return from the TP-Layer!

        public Instance(int tpInstance, int range_min, int range_max) {
            this.range_min = range_min;
            this.range_max = range_max;

            if (enable_work_stealing)
                executor = Executors.newWorkStealingPool(tpInstance);
            else
                executor = Executors.newFixedThreadPool(tpInstance);
        }

        /**
         * Single box instance. Shared everything.
         *
         * @param tp
         */
        public Instance(int tp) {
            if (tp == 0) {
                executor = Executors.newCachedThreadPool();
            } else
                executor = Executors.newWorkStealingPool(tp);
//            TP_THREADS = new ExecutorCompletionService(executor);
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }

    class dummyTask implements Callable<Integer> {
        int socket;

        public dummyTask(int socket) {
            this.socket = socket;
        }

        @Override
        public Integer call() {
            runOnNode(socket);
            return null;
        }
    }


    //the smallest unit of Task in TP.
    //Every Task will be assigned with one operation chain.
    class Task implements Callable<Integer> {
        private AtomicBoolean under_process = new AtomicBoolean(false);
        private final Set<Operation> operation_chain;
        private int socket;
        private final long wid = 0;//watermark id.
        private boolean un_processed = true;

        public Task(Set<Operation> operation_chain, long bid, int socket) {

            this.operation_chain = operation_chain;
//            wid = bid;

            this.socket = socket;
        }

        public Task(Set<Operation> operation_chain, long bid) {

            this.operation_chain = operation_chain;

        }

        /**
         * TODO: improve this function later, so far, no application requires this...
         *
         * @param operation
         */
        private void wait_for_source(Operation operation) {
//            ConcurrentSkipListSet<numa_engine.common.Operation> source_operation_chain = holder_v1.get(Operation.s_record.record_.GetPrimaryKey());
//            //if the source_operation_chain is completely evaluated, we are safe to proceed. This is the most naive approach.
//            while (!source_operation_chain.isEmpty()) {
//                //Think about better shortcut.
//            }
        }

        //evaluate the operation_chain
        //TODO: paralleling this process.
        @Override
        public Integer call() {

//            if (enable_work_stealing) {//cannot ensure ordering
//                process((MyList<Operation>) operation_chain);
//                return 0;
//            }
//            else {

            if (this.under_process.compareAndSet(false, true)) {//ensure one task is processed only once.
//                int i = 0;
                if (enable_debug)
                    LOG.info("Thread:\t" + Thread.currentThread().getName()
                            + "\t working on task:" + OsUtils.Addresser.addressOf(this)
                            + " with size of:" + operation_chain.size());
                process((MyList<Operation>) operation_chain);
                if (enable_debug)
                    LOG.info("Thread:\t" + Thread.currentThread().getName()
                            + "reset task:" + OsUtils.Addresser.addressOf(this));
                operation_chain.clear();
                this.under_process.set(false);//reset
                return 0;
            }
            if (enable_debug)
                LOG.info("Thread:\t" + Thread.currentThread().getName()
                        + "\t exit on task:" + OsUtils.Addresser.addressOf(this)
                        + " with size of:" + operation_chain.size());
            while (!this.under_process.compareAndSet(false, true)) ;
            return 0;
//            }
        }

    }

}