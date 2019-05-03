package brisk.execution.runtime;

import applications.Platform;
import applications.util.Configuration;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionNode;
import ch.usi.overseer.OverHpc;
import engine.DatabaseException;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static brisk.controller.affinity.SequentialBinding.next_cpu;
import static xerial.jnuma.Numa.*;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public abstract class executorThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(executorThread.class);

    public final ExecutionNode executor;
    protected final CountDownLatch latch;
    final Configuration conf;
    private final HashMap<Integer, executorThread> threadMap;
    private final OverHpc hpcMonotor;
    public boolean running = true;
    public boolean profiling = false;
    public long[] cpu;
    public int node;
    public boolean migrating = false;
    protected AffinityLock lock;
    double expected_throughput = 0;
    boolean not_yet_profiled = true;
    TopologyContext context;//every thread owns its unique context, which will be pushed to its emitting tuple.
    double cnt = 0;
    long start_emit = 0;
    long end_emit = 0;
    int batch;
    private boolean start = true;
    private volatile boolean ready = false;

    protected executorThread(ExecutionNode e, Configuration conf, TopologyContext context
            , long[] cpu, int node, CountDownLatch latch, OverHpc HPCMonotor, HashMap<Integer, executorThread> threadMap) {
        this.context = context;
        this.conf = conf;
        executor = e;
        this.cpu = cpu;
        this.node = node;
        this.latch = latch;
        hpcMonotor = HPCMonotor;
        this.threadMap = threadMap;

        if (executor != null && !this.executor.isLeafNode()) {
            this.executor.getController().setContext(this.executor.getExecutorID(), context);
        }
    }

    public TopologyContext getContext() {
        return context;
    }

    public void setContext(TopologyContext context) {
        this.context = context;
    }

    private long[] convertToCPUMasK(long[] cpu) {
        final long[] cpuMask = newCPUBitMask();
        LOG.info("Empty cpuMask:" + Arrays.toString(cpuMask));

        for (long i : cpu) {
            cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a single CPU on
        }
        LOG.info("Configured cpuMask:" + Arrays.toString(cpuMask));
        return cpuMask;
    }


    protected long[] sequential_binding() {


        setLocalAlloc();
        int cpu = next_cpu();
//        lock_ratio = AffinityLock.acquireLock(cpu);

        AffinityLock.acquireLock(cpu);
        LOG.info(this.executor.getOP_full() + " binding to node:" + node + " cpu:" + cpu);

//        return AffinityLock.dumpLocks();

        return null;
    }

    /**
     * This will be called only once when thread is launching..
     *
     * @return
     */
    protected long[] binding() {

//        if (node != -1 && node != 0) {
//			setAffinity(convertToCPUMasK(cpu));
//			setLocalAlloc();
//		runOnNode(node);
//		setPreferred(node);
//        }
//		return getAffinity();
        //return rebinding_clean();

//		try {

//			ClassLoader classLoader = getClass().getClassLoader();
//			File file = new File(classLoader.getResource("HUAWEI.cpuinfo").getFile());
//
//			LockInventory lockInventory = new LockInventory(VanillaCpuLayout.fromCpuInfo(file));
//		new AffinityLock(0, true, false, lockInventory);

        setLocalAlloc();
        if (executor != null) {
            LOG.info(this.executor.getOP_full());
        } else {
            LOG.info("optimization manager:");
        }

        LOG.info(" binding to node:" + node + " cpu:" + (int) cpu[0]);//+ " and " + ((int) cpu[0] + 144)););

        lock = AffinityLock.acquireLock((int) cpu[0]);

//		if (!OsUtils.isMac()) {
//			lock_ratio = AffinityLock.acquireLock((int) cpu[0] + 144);//the HT thread.
//		}
//			lock_ratio = new AffinityLock((int) cpu[0], false, false, lockInventory);
//			lock_ratio.bind(true);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
        return getAffinity();

    }

    /**
     * this may be called multiple times..
     *
     * @return
     */
    private long[] rebinding_clean() {
//		int bufSize = (context.getGraph().topology.getPlatform().num_cores + 64 - 1) / 64;//because the change of affinity, num_cpu() will change..
//		long[] cpuMask = new long[bufSize];
//		for (long i : cpu) {
//			cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a single CPU on
//		}
//		runOnAllNodes();
//		setAffinity(cpuMask);

//		runOnNode(node);
//		setPreferred(node);
        setAffinity(convertToCPUMasK(cpu));
        setLocalAlloc();

        return getAffinity();
    }

    /**
     * this may be called multiple times..
     *
     * @return
     */
    public long[] rebinding() {

        int bufSize = (context.getGraph().topology.getPlatform().num_cores + 64 - 1) / 64;//because the change of affinity, num_cpu() will change..
        long[] cpuMask = new long[bufSize];
        LOG.info("Newly created:" + context.getGraph().topology.getPlatform().num_cores);
//        LOG.info(Arrays.show(cpuMask));

        try {
            for (long i : cpu) {
                cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a partition CPU on
            }
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            LOG.info("Problematic");
            LOG.info(executor.getOP() + ":" + Arrays.toString(cpuMask));
//            LOG.info(Arrays.show(cpu));
            //System.exit(-1);
        }
//        LOG.info("Normal");
//        LOG.info(Arrays.show(cpuMask));
//        LOG.info(Arrays.show(cpu));

        try {
            setAffinity(cpuMask);
        } catch (java.lang.Exception e) {
            LOG.info("Problematic");
            LOG.info(executor.getOP() + ":" + Arrays.toString(cpuMask));
//            LOG.info(Arrays.show(cpu));
//            System.exit(-1);
        }
        LOG.info("Normal");
        LOG.info(executor.getOP() + ":" + Arrays.toString(cpuMask));
//        runOnNode(node);
        return getAffinity();
    }

    public void initilize_queue(int executorID) {
        allocate_OutputQueue();
        assign_InputQueue();
    }

    private void _migrate() {
        //TODO: disable migration for now.... We only profile once..
        if (migrating) {
            executorThread.LOG.info(this.executor.getOP() + " start migrating");
            rebinding_clean();
            not_yet_profiled = true;
            this.pause_parent();
            //sync_ratio until all output queues become empty here.
            while (!this.executor.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOG.info(this.executor.getOP() + " has no outputs now, recreates its output queues now!");
            //pause its consumers.

            this.pause();
            initilize_queue(this.executor.getExecutorID());
            executorThread.LOG.info(this.executor.getOP() + " migrating complete");
            this.restart();
            this.restart_parents();
//            Thread.sleep(migration_gaps);
            migrating = false;
        }
    }

    private void pause() {
        for (TopologyComponent children : this.executor.getChildren_keySet()) {
            for (ExecutionNode c : children.getExecutorList()) {
                if (threadMap.get(c.getExecutorID()) != null) {
                    threadMap.get(c.getExecutorID()).suspend();
                } else {
                    LOG.info(c.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void pause_parent() {
        for (TopologyComponent parent : this.executor.getParents_keySet()) {
            for (ExecutionNode p : parent.getExecutorList()) {
                if (threadMap.get(p.getExecutorID()) != null) {
                    threadMap.get(p.getExecutorID()).suspend();
                } else {
                    LOG.info(p.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void restart() {
        for (TopologyComponent children : this.executor.getChildren_keySet()) {
            for (ExecutionNode c : children.getExecutorList()) {
                if (threadMap.get(c.getExecutorID()) != null) {
                    threadMap.get(c.getExecutorID()).resume();
                } else {
                    LOG.info(c.getOP() + " do not have threads.");
                }
            }
        }
    }

    private void restart_parents() {
        for (TopologyComponent parent : this.executor.getParents_keySet()) {
            for (ExecutionNode p : parent.getExecutorList()) {
                if (threadMap.get(p.getExecutorID()) != null) {
                    threadMap.get(p.getExecutorID()).resume();
                } else {
                    LOG.info(p.getOP() + " do not have threads.");
                }
            }
        }
    }

    public void migrate(long[] cpu) {
        migrating = true;//sync_ratio to be scheduled.
//        LOG.info("Old CPU:" + Arrays.show(this.cpu));
        this.cpu = cpu;
//        LOG.info("New CPU:" + Arrays.show(this.cpu));
    }

    public void migrate(int node) {
        migrating = true;//sync_ratio to be scheduled.
        this.node = node;
    }

    private void allocate_OutputQueue() {
//        if (enable_latency_measurement) {
//            executor.allocate_OutputQueue(conf.getBoolean("linked", false), 2);//no queueing delay.
//        } else {
        executor.allocate_OutputQueue(conf.getBoolean("linked", false), (int) (conf.getInt("targetHz") * conf.getDouble("checkpoint")));
//        }
    }

    private void assign_InputQueue(String streamId) {
        executor.setReceive_queueOfChildren(streamId);
    }

    /**
     * Assign my output queue to my downstream executor.
     */
    private void assign_InputQueue() {

        for (String streamId : executor.operator.getOutput_streamsIds()) {
            assign_InputQueue(streamId);
        }
    }

    HashMap<Integer, Queue> get_receiving_queue(String streamId) {
        return executor.getInputStreamController().getReceive_queue(streamId);
    }

    HashMap<String, HashMap<Integer, Queue>> get_receiving_queue() {
        return executor.getInputStreamController().getRQ();
    }

    void profile_routing(Platform p) throws InterruptedException, DatabaseException, BrokenBarrierException {
        executor.prepareProfilingStruct(conf, hpcMonotor, context, p);
        if (start) {
            cnt = 0;
            start_emit = System.nanoTime();
            start = false;
        }
        while (running) {
            if (TopologyContext.plan.isProfile(executor.getExecutorID()) && not_yet_profiled) {
                _profile();
            } else {
                _execute();
            }
            _migrate();
        }//thread exist running
        end_emit = System.nanoTime();
    }

    void routing() throws InterruptedException, DatabaseException, BrokenBarrierException {
//        int s = 0;
        if (start) {
            cnt = 0;
            start_emit = System.nanoTime();
            start = false;
        }
        while (running) {
            _execute();
//			_migrate(LOG);//even if this thread is not under measurement, it may still need to be re-scheduled.
//            if (s++ % 10000 == 0)
//                this.executor.op.display();
//            Thread.yield();
        }
        end_emit = System.nanoTime();
    }

    protected abstract void _execute_noControl() throws InterruptedException, DatabaseException, BrokenBarrierException;

    protected abstract void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException;

    protected abstract void _profile() throws InterruptedException, DatabaseException, BrokenBarrierException;

    public int getExecutorID() {
        return executor.getExecutorID();
    }

    public String getOP() {
        return executor.getOP();
    }

    public double getResults() {
        return executor.op.getResults();
    }

    public boolean isReady() {
        return ready;
    }

    void Ready(Logger LOG) {
        //LOG.DEBUG("BasicBoltBatchExecutor:" + executor.getExecutorID() + " is set to ready");
        ready = true;
    }

//    public void incParallelism() {
//        executor.op.parallelism++;
//    }
}
