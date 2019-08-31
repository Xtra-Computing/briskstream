package brisk.execution.runtime;

import applications.util.Configuration;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
import brisk.components.operators.executor.BoltExecutor;
import brisk.controller.input.InputStreamController;
import brisk.execution.Clock;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.optimization.OptimizationManager;
import brisk.optimization.model.STAT;
import ch.usi.overseer.OverHpc;
import com.javamex.classmexer.MemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static applications.util.OsUtils.isUnix;
import static com.javamex.classmexer.MemoryUtil.VisibilityFilter.ALL;
import static net.openhft.affinity.AffinityLock.dumpLocks;

/**
 * Task thread that hosts bolt logic. Receives input Brisk.execution.runtime.tuple,
 * processes it using bolt logic, forwards to next bolts in
 * Brisk.topology.
 */
public class boltThread extends executorThread {

    private final static Logger LOG = LoggerFactory.getLogger(boltThread.class);
    private final BoltExecutor bolt;
    private final OutputCollector collector;
    private final InputStreamController scheduler;

    public volatile boolean binding_finished = false;
    private boolean UNIX = false;
    private int miss = 0;

    /**
     * @param e
     * @param context
     * @param conf
     * @param cpu
     * @param node
     * @param latch
     * @param HPCMonotor
     * @param optimizationManager
     * @param threadMap
     * @param clock
     */
    public boltThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu
            , int node, CountDownLatch latch, OverHpc HPCMonotor, OptimizationManager optimizationManager
            , HashMap<Integer, executorThread> threadMap, Clock clock) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        bolt = (BoltExecutor) e.op;
        scheduler = e.getInputStreamController();
        this.collector = new OutputCollector(e, context);
        batch = conf.getInt("batch", 100);
        bolt.setExecutionNode(e);
        bolt.setclock(clock);
    }

    protected void _profile() throws InterruptedException, BrokenBarrierException {
        if (isUnix()) {
            UNIX = true;
            // LOG.info("running in Linux environment");
        }
        long time_out = (long) (60 * 1E3);
        for (TopologyComponent parent : executor.getParents_keySet()) {
//			for (ExecutionNode src : parent.getExecutorList()) {
            ExecutionNode src = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);//use middle one as producer..
            int srcExecutorID = src.getExecutorID();
            double loop = executor.profiling.get(srcExecutorID).loop;
            STAT stat = executor.profiling.get(srcExecutorID);
            cnt = 0;
            long start = System.currentTimeMillis();
            for (int i = 0; i < loop && running; i++) {
//				TransferTuple in = fetchResult(src, stat, batch);//special fetch, only fetch targeted source for profiling purpose.
//                    LOG.info(this.executor.getOP() + "\t" + this.executor.getExecutorID() + " Processed:" + processed);

                TransferTuple in = fetchResult(stat, batch);
                if (in != null) {
                    if (in.getSourceTask() == srcExecutorID && i > 30000) {//skip the non-compiled optimized part.

                        stat.start_measure();
                        bolt.profile_execute(in);//skip the blocking in emitting.
//						if (cnt % 1E3 == 0) {
//							System.gc();//force a gc?
//						}

                        stat.end_measure(batch);
                        cnt += batch;
                        long size_of_tuple = 0;

//							for (int index = 0; index < batch; index++) {
//								if (UNIX) {
                        size_of_tuple = MemoryUtil.deepMemoryUsageOf(in.msg[0].getValue(), ALL);
//								}
                        assert size_of_tuple >= 0;
                        stat.setProfiling(true, size_of_tuple);
//							}
                    } else {
                        bolt.execute(in);
                    }
                } else {
                    i--;//make sure only the actual execution is measured.
                    //LOG.info("BasicBoltBatchExecutor:" + this.executor.operator.id+"
                    //"+ this.executor.getExecutorIDList() +" make sure only the actual execution is measured.");
                    long end = System.currentTimeMillis();
                    if ((end - start) > time_out) {
                        LOG.info("Wait for profiling gaps" + time_out / 1E3 + " (s)...for executor:"
                                + this.executor.getOP_full() + "on source:"
                                + src.getOP_full() + " for too long, force to exist");
                        break;
                    }
                }
            }
            LOG.info(this.executor.getOP_full() + "\tfinished profiling for source:\t"
                    + src.getOP_full() + "\t" + " on node: " + node);
        }
        profiling = false;
        not_yet_profiled = false;
    }


    /**
     * @author Crunchify.com
     */

    public static String crunchifyGenerateThreadDump() {
        final StringBuilder dump = new StringBuilder();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    /**
     * Be very careful for this method..
     *
     * @throws InterruptedException
     */
    protected void _execute_noControl() throws InterruptedException, BrokenBarrierException {

        TransferTuple in = fetchResult();
        if (in != null) {
            bolt.execute(in);
            cnt += batch;
        } else {
            miss++;
        }


    }

    protected void _execute() throws InterruptedException, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    public void run() {

        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());
            if (TopologyContext.plan.getSP() != null) {
                for (String Istream : new HashSet<>(executor.operator.input_streams)) {
//					for (String Ostream : new HashSet<>(executor.operator.getOutput_streamsIds())) {
                    expected_throughput += executor.getExpectedProcessRate(Istream, TopologyContext.plan.getSP(), false) * 1E6;
//					}
                }
            }
            Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            long[] binding = null;
            if (!conf.getBoolean("NAV", true)) {
                binding = binding();
            }

            initilize_queue(this.executor.getExecutorID());

            //do preparation.
            bolt.prepare(conf, context, collector);

            this.Ready(LOG);


            binding_finished = true;


            if (binding != null) {
                LOG.info("Successfully create boltExecutors " + bolt.getContext().getThisTaskId()
                        + "\tfor bolts:" + executor.getOP()
                        + " on node: " + node
                        + "binding:" + Long.toBinaryString(0x1000000000000000L | binding[0]).substring(1)
                );
            }
//            controllerThread ct = new controllerThread(this);

            latch.countDown();          //tells others I'm ready.
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            ct.start();
            if (isUnix()) {
                boolean UNIX = true;
                // LOG.info("running in Linux environment");
            }
            if (
//					conf.getBoolean("profile", false)
//					&& this.executor.getExecutorID() <= optimizationManager.end_executor
//					&& this.executor.getExecutorID() >= optimizationManager.start_executor
                    this.executor.needsProfile()//actual runs while profiling certain operators.
            ) {
                profile_routing(
                        context.getGraph().topology.getPlatform()
                );
            } else {
                routing();
            }
        } catch (InterruptedException | BrokenBarrierException ignored) {
        } finally {
            if (lock != null) {
                lock.release();
            }
            this.executor.display();
            if (end_emit == 0) {
                end_emit = System.nanoTime();
            }
            double actual_throughput = cnt * 1E6 / (end_emit - start_emit);
            if (expected_throughput == 0) {
                expected_throughput = actual_throughput;
            }

            LOG.info(this.executor.getOP_full()
                            + "\tfinished execution and exist with throughput of:\t"
                            + actual_throughput + "(" + (actual_throughput / expected_throughput) + ")"
                            + " on node: " + node + " fetch miss rate:" + miss / (cnt + miss) * 100
//					+ " ( " + Arrays.show(cpu) +")"
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {

            }
        }
    }

    /**
     * Get input from upstream bolts, this is a unique function of bolt thread.
     * TODO: need a txn module to determine the fetch sequence.
     *
     * @since 0.0.7 we add a tuple txn module so that we can support customized txn rules in Brisk.execution.runtime.tuple fetching.
     */
    private TransferTuple fetchResult() {

        return scheduler.fetchResults();
//		return scheduler.fetchResults_inorder();


    }

    private Tuple fetchResult_single() {

        return scheduler.fetchResults_single();
//		return scheduler.fetchResults_inorder();


    }


    private TransferTuple fetchResult(STAT stat, int batch) {
        return scheduler.fetchResults(stat, batch);
    }


    private TransferTuple fetchResult(ExecutionNode src, STAT stat, int batch) {
        return scheduler.fetchResults(src, stat, batch);
    }

}
