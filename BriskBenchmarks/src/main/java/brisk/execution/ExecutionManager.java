package brisk.execution;

import Platform;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
import brisk.components.exception.UnhandledCaseException;
import brisk.controller.affinity.AffinityController;
import brisk.execution.runtime.boltThread;
import brisk.execution.runtime.executorThread;
import brisk.execution.runtime.spoutThread;
import brisk.faulttolerance.Writer;
import brisk.optimization.ExecutionPlan;
import brisk.optimization.OptimizationManager;
import ch.usi.overseer.OverHpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import static Constants.EVENTS.*;
import static Constants.*;
import static xerial.jnuma.Numa.*;

/**
 * Created by shuhaozhang on 19/8/16.
 */
public class ExecutionManager {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);
    private final static long migration_gaps = 10000;
    //    public static Clock clock = null;
    public final HashMap<Integer, executorThread> ThreadMap = new HashMap<>();
    public final AffinityController AC;
    private final OptimizationManager optimizationManager;
    private int loadTargetHz;
    private int timeSliceLengthMs;
    private OverHpc HPCMonotor;
    private ExecutionGraph g;


    public ExecutionManager(ExecutionGraph g, Configuration conf, OptimizationManager optimizationManager, Platform p) {
        this.g = g;
        AC = new AffinityController(conf, p);
        this.optimizationManager = optimizationManager;
        initializeHPC();
    }

    /**
     * CPU_CLK_UNHALTED.REF
     * MicroEvent Code: 0x00
     * <p>
     * Mask: 0x00
     * <p>
     * Category: Basic Events;All Events;
     * <p>
     * Definition: Reference cycles when core is not halted.
     * <p>
     * Description: This event counts the number of reference cycles that the core is not in a halt state. The core enters the halt state when it is running the HLT instruction.
     * In mobile systems the core frequency may change from time. This event is not affected by core frequency changes but counts as if the core is running at the maximum frequency all the time. This event has a constant ratio with the CPU_CLK_UNHALTED.BUS event.
     * Divide this event count by core frequency to determine the elapsed time while the core was not in halt state.
     * Note: The event CPU_CLK_UNHALTED.REF is counted by a designated fixed timestamp_counter, leaving the two programmable counters available for other events.
     */


    private void initializeHPC() {
//        if (isUnix()) {
        try {
            HPCMonotor = OverHpc.getInstance();
            if (HPCMonotor == null) {
                LOG.info("ERROR: unable to init OverHpc");
            }

            // Init event: LLC miss for memory fetch. + "," + LLC_PREFETCHES+ "," + L1_ICACHE_LOADS
            if (!HPCMonotor.initEvents(
                    LLC_MISSES
                            + "," + LLC_REFERENCES
                            + "," + PERF_COUNT_HW_CPU_CYCLES
//								+ "," + L1_ICACHE_LOAD_MISSES
//								+ "," + L1_DCACHE_LOAD_MISSES
            )) {
                LOG.error("ERROR: invalid event");
            }
        } catch (java.lang.UnsatisfiedLinkError e) {
            System.out.println("ERROR: unable to init OverHpc. " + e.getMessage());
            HPCMonotor = null;
        }
//        }
    }

    /**
     * Launch threads for each executor in executionGraph
     * We make sure no interference among threads --> one thread one core.
     * TODO: let's think about how to due with multi-thread per core in future..
     * All executors have to wait for OM to start, so it's safe to do initialization here. E.g., initialize database.
     */
    public void distributeTasks(Configuration conf,
                                ExecutionPlan plan, CountDownLatch latch, boolean benchmark, boolean profile, Platform p) throws UnhandledCaseException {
        assert plan != null;
        loadTargetHz = (int) conf.getDouble("targetHz", 10000000);
        LOG.info("Finally, targetHZ set to:" + loadTargetHz);
        timeSliceLengthMs = conf.getInt("timeSliceLengthMs");

        if (plan.getSP() != null) {

            this.g = plan.getSP().graph;

//			if (conf.getBoolean("backPressure", false)) {
//				conf.put("targetHz", plan.getSP().variables.SOURCE_RATE * 1E9);
//			}
        }
        g.build_inputScheduler();
//        clock = new Clock(conf.getDouble("checkpoint", 1));

        if (conf.getBoolean("Fault_tolerance", false)) {
            Writer writer = null;
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                if (e.isFirst_executor()) {
                    writer = new Writer(e.operator, e.operator.getNumTasks());
                }
                e.configureWriter(writer);
            }
        }

        executorThread thread = null;
        if (benchmark) {
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                switch (e.operator.type) {
                    case spoutType:
                        thread = launchSpout_InCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor), conf
                                , plan.toSocket(e.getExecutorID()), plan.getSP().allowedCores(), latch);
                        break;
                    case boltType:
                    case sinkType:
                        thread = launchBolt_InCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor), conf
                                , plan.toSocket(e.getExecutorID()), plan.getSP().allowedCores(), latch);
                        break;
                    case virtualType:
                        LOG.info("Won't launch virtual ground");
                        if (!conf.getBoolean("NAV", true)) {
                            e.prepareProfilingStruct(conf, null, null, p);
                        }
                        break;
                    default:
                        throw new UnhandledCaseException("type not recognized");
                }

                if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
                    while (thread != null && !thread.isReady()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
                //LOG.DEBUG("Operator:\t" + e.getOP() + " is ready");
            }
        } else if (profile) {
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                switch (e.operator.type) {
                    case spoutType:
                        thread = launchSpout_SingleCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor), conf
                                , plan.toSocket(e.getExecutorID()), latch);
                        break;
                    case boltType:
                    case sinkType:
                        thread = launchBolt_SingleCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor), conf
                                , plan.toSocket(e.getExecutorID()), latch);
                        break;
                    case virtualType:
                        LOG.info("Won't launch virtual ground");
                        if (!conf.getBoolean("NAV", true)) {
                            e.prepareProfilingStruct(conf, null, null, p);
                        }
                        break;
                    default:
                        throw new UnhandledCaseException("type not recognized");
                }
                if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
                    while (thread != null && !thread.isReady()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
                //LOG.DEBUG("Operator:\t" + e.getOP() + " is ready");
            }
        } else {

            TopologyComponent previous_op = null;
            long start = System.currentTimeMillis();
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {

                switch (e.operator.type) {
                    case spoutType:
                        thread = launchSpout_SingleCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor)
                                , conf, plan.toSocket(e.getExecutorID()), latch);
                        break;
                    case boltType:
                    case sinkType:
                        thread = launchBolt_SingleCore(e, new TopologyContext(g, plan, e, ThreadMap, HPCMonotor)
                                , conf, plan.toSocket(e.getExecutorID()), latch);
                        break;
                    case virtualType:
                        LOG.info("Won't launch virtual ground");
                        if (!conf.getBoolean("NAV", true)) {
                            e.prepareProfilingStruct(conf, null, null, p);
                        }
                        break;
                    default:
                        throw new UnhandledCaseException("type not recognized");
                }

//                if (previous_op == null || e.operator != previous_op) {


                if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
                    assert thread != null;
                    while (!thread.isReady()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
//                            ex.printStackTrace();
                        }
                    }
                }
//                }
//                previous_op = e.operator;
            }
            long end = System.currentTimeMillis();
            LOG.info("It takes :" + (end - start) / 1000 + " seconds to finish launch the operators.");
        }
    }


    private executorThread launchSpout_InCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                              int node, long[] cores, CountDownLatch latch) {
        spoutThread st;

        st = new spoutThread(e, context, conf, cores, node, latch, loadTargetHz, timeSliceLengthMs
                , HPCMonotor, ThreadMap);

        st.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            st.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), st);
        return st;
    }

    private executorThread launchBolt_InCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                             int node, long[] cores, CountDownLatch latch) {

        boltThread wt;
        wt = new boltThread(e, context, conf, cores, node, latch,
                HPCMonotor, optimizationManager, ThreadMap);
        wt.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            wt.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), wt);
//		try {
//			while (!wt.binding_finished) {
//				Thread.sleep(500);//wait for queue allocation.
//			}
//		} catch (InterruptedException e1) {
//			e1.printStackTrace();
//		}
        return wt;
    }

    private executorThread launchSpout_bySocket(ExecutionNode e, TopologyContext context, Configuration conf,
                                                int node, CountDownLatch latch) {

        long[] cpu = AC.require(node);
//		LOG.info("Launch spout on cpu:" + Arrays.show(cpu));
        return launchSpout_InCore(e, context, conf, node, cpu, latch);
    }

    private executorThread launchSpout_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                  int node, CountDownLatch latch) {
        spoutThread st;
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }
//		LOG.info("Launch spout on cpu:" + Arrays.show(cpu));
        return launchSpout_InCore(e, context, conf, node, cpu, latch);
    }

    private executorThread launchBolt_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                 int node, CountDownLatch latch) {

//		LOG.info("Launch bolt:" + e.getOP() + " on node:" + node);
        long[] cpu;
        if (!conf.getBoolean("NAV", true)) {
            cpu = AC.requirePerCore(node);
        } else {
            cpu = new long[1];
        }
        return launchBolt_InCore(e, context, conf, node, cpu, latch);
    }

    private executorThread launchBolt_bySocket(ExecutionNode e, TopologyContext context, Configuration conf,
                                               int node, CountDownLatch latch) {
//		LOG.info("Launch bolt:" + e.getOP() + " on node:" + node);
        long[] cpu = AC.require(node);
        return launchBolt_InCore(e, context, conf, node, cpu, latch);
    }


    private boolean migrate_complete(executorThread thread) {
        return !thread.migrating;
    }

    private boolean migrate_complete(ExecutionGraph g) {
        for (executorThread thread : ThreadMap.values()) {
            if (thread.migrating) {
                return false;//still during migration.
            }
        }
        return true;
    }

    public void redistributeTasks(ExecutionGraph g, Configuration conf, ExecutionPlan plan)
            throws InterruptedException {
        LOG.info("BasicBoltBatchExecutor rebinding..");

        AC.clear();
//		TopologyContext[] contexts = new TopologyContext[g.getExecutionNodeArrayList().size() - 1];
//		int i = 0;
        for (executorThread thread : ThreadMap.values()) {
            int toSocket = plan.toSocket(thread.getExecutorID());
            long[] cpu = AC.require(toSocket);
            thread.migrate(cpu);
            thread.migrate(toSocket);
            while (true) {
                if (migrate_complete(thread)) {
                    break;
                }
                Thread.sleep(1000);
            }
            LOG.info("Rebind Executors " + thread.getOP() + "-" + thread.getExecutorID() + " on core: " + Arrays.toString(cpu));
            TopologyContext.plan = plan;//update context.
//			contexts[i] = new TopologyContext(g, null, plan, thread.executor, ThreadMap, HPCMonotor);
//			thread.setContext(contexts[i]);
//			i++;
        }


        LOG.info("At this point, all threads are re-scheduled successfully.");
//        pause();
//        recreates_queue();
//        resume();
//        LOG.info("Re-binding complete");
        LOG.info("Migration complete");
    }

//    long[] rebinding(int[] cpu) {
//        long[] cpuMask = newCPUBitMask();
//
//        for (int i : cpu)
//            cpuMask[i / 64] |= 1L << (i % 64);
//
//        setAffinity(cpuMask);
//        setLocalAlloc();
//        return getAffinity();
//    }

    private long[] rebinding(long[] cpu) {

        int bufSize = (g.topology.getPlatform().num_cores + 64 - 1) / 64;//because the change of affinity, num_cpu() will change..
        long[] cpuMask = new long[bufSize];
        LOG.info("Newly created:" + g.topology.getPlatform().num_cores);
//        LOG.info(Arrays.show(cpuMask));

        try {
            for (long i : cpu) {
                cpuMask[(int) (i / 64)] |= 1L << (i % 64); //Create a bit mask setting a partition CPU on
            }
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            LOG.info("Problematic");
            LOG.info("EM:" + Arrays.toString(cpuMask));
//            LOG.info(Arrays.show(cpu));
//            System.exit(-1);
        }
//        LOG.info("Normal");
//        LOG.info(Arrays.show(cpuMask));
//        LOG.info(Arrays.show(cpu));

        try {
            setAffinity(cpuMask);
        } catch (java.lang.Exception e) {
            LOG.info("Problematic");
            LOG.info("EM:" + Arrays.toString(cpuMask));
//            LOG.info(Arrays.show(cpu));
//            System.exit(-1);
        }
        LOG.info("Normal");
        LOG.info("EM:" + Arrays.toString(cpuMask));
//        runOnNode(node);
        return getAffinity();
    }

    public void recreates_queue() {
        for (executorThread thread : ThreadMap.values()) {
            long[] rebinding = this.rebinding(thread.cpu);
//            thread.cpu = rebinding;
            LOG.info("Recreates queue for " + thread.getOP() + ", Bind EM to " + " on core: " + Arrays.toString(thread.cpu) + "binding:"
                    + Long.toBinaryString(0x1000000000000000L | rebinding[0]).substring(1));
            thread.initilize_queue(thread.getExecutorID());
        }
        System.gc();
        LOG.info("Re-creates the queues.");
        resetAffinity();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void resume() {
        for (executorThread thread : ThreadMap.values()) {
            thread.resume();
        }
        LOG.info("Resume all threads.");
    }

    public void pause() {
        for (executorThread thread : ThreadMap.values()) {
            thread.suspend();
        }
        LOG.info("Pause all threads.");
    }

    /**
     * stop EM
     * It stops all execution threads as well.
     */
    public void exist() {
        LOG.info("Execution stops.");
        this.getSinkThread().getContext().Sequential_stopAll();
    }

    public executorThread getSinkThread() {
        return ThreadMap.get(g.getSinkThread());
    }
}
