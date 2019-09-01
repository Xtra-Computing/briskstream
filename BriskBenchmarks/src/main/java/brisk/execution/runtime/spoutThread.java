package brisk.execution.runtime;

import applications.Constants;
import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import brisk.components.operators.executor.BasicSpoutBatchExecutor;
import brisk.execution.Clock;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.optimization.model.STAT;
import ch.usi.overseer.OverHpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;


/**
 * Task thread that hosts spout logic.
 */
public class spoutThread extends executorThread {

    private static final Logger LOG = LoggerFactory.getLogger(spoutThread.class);
    private final BasicSpoutBatchExecutor sp;
    private final int loadTargetHz;
    private final int timeSliceLengthMs;
    private final int elements;
    private final OutputCollector collector;
    int sleep_time = 0;
    int busy_time = 0;

    /**
     * @param e                 :                  Each thread corresponds to one executionNode.
     * @param conf
     * @param cpu
     * @param node
     * @param latch
     * @param loadTargetHz
     * @param timeSliceLengthMs
     * @param HPCMonotor
     * @param threadMap
     */
    public spoutThread(ExecutionNode e, TopologyContext context, Configuration conf, long[] cpu,
                       int node, CountDownLatch latch, int loadTargetHz, int timeSliceLengthMs, OverHpc HPCMonotor,
                       HashMap<Integer, executorThread> threadMap) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        this.sp = (BasicSpoutBatchExecutor) e.op;
        this.loadTargetHz = loadTargetHz;
        this.timeSliceLengthMs = timeSliceLengthMs;
        this.collector = new OutputCollector(e, context);
        batch = conf.getInt("batch", 100);
        elements = loadPerTimeslice();//how many elements are required to sent each time.
        sp.setExecutionNode(e);
    }

    @Override
    protected void _execute_noControl() throws InterruptedException {
        sp.bulk_emit(batch);
        cnt += batch;
    }

    protected void _execute() throws InterruptedException {
//         emit_withControl();
        _execute_noControl();
        //sp.nextTuple();
    }

    protected void _profile() throws InterruptedException {
        STAT stat = executor.profiling.get(-1);
        double loop = stat.loop;
//		int repeat = 1000;
//        profiling = true;
//                int repeate = 1;
        for (int i = 0; i < loop; i++) {
//            executor.profiling.get(-1).start_measure(1);
//            stat.start_measure(1);
//                    for (int r = 0; r < repeate; r++)
            stat.start_measure();
//			for (int j = 0; j < repeat; j++) {
            sp.bulk_emit(batch);
//			}

            stat.end_measure(batch);//* repeat
//            stat.end_measure();
//                    executor.profiling.get(-1).end_measure();
            stat.setProfiling(
                    true//it must be local for spout thread.
                    , 0);
        }
        cnt += loop * batch;
        LOG.info(this.executor.getOP_full() + " finished all profiling" + " on node: " + node);
        profiling = false;
        not_yet_profiled = false;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName("Operator:" + executor.getOP() + "\tExecutor ID:" + executor.getExecutorID());

            long[] binding = null;
            if (!conf.getBoolean("NAV", true)) {
                binding = binding();
            }

            initilize_queue(this.executor.getExecutorID());
            boolean binding_finished = true;
            //do Loading


            sp.prepare(conf, context, collector);
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
//            Thread.currentThread().setDaemon(true);

            if (binding != null) {
                LOG.info("Successfully create spoutExecutors " + sp.getContext().getThisTaskId() + " on node: " +
                        "" + node
                        + "binding:" + Long.toBinaryString(0x1000000000000000L | binding[0]).substring(1)
                );
            }

            this.Ready(LOG);//Tell executor thread to proceed.

            LOG.info("Operator:\t" + executor.getOP_full() + " is ready");

            //pre-loading input tuples.
//            int preload_size = conf.getInt("targetHz") * conf.getInt("checkpoint") * conf.getInt("tthread") / 2;
//            sp.bulk_emit(preload_size);
//            cnt += preload_size;

            System.gc();
            latch.countDown();          //tells others I'm really ready.
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }


            if (
//					conf.getBoolean("profile", false)
//					&& this.executor.getExecutorID() <= optimizationManager.end_executor
//					&& this.executor.getExecutorID() == optimizationManager.start_executor
                    this.executor.needsProfile()//actual runs while profiling certain operators.
            ) {
//                sp.getCollector().setNo_wait(true);//let it be always allowed.
                profile_routing(context.getGraph().topology.getPlatform());
//                sp.getCollector().setNo_wait(false);
            } else {
                routing();
            }
        } catch (InterruptedException | BrokenBarrierException ignored) {
//            e.printStackTrace();
        } finally {
            if (lock != null) {
                lock.release();
            }
            this.executor.display();
            double expected_throughput = 0;
            if (end_emit == 0) {
                end_emit = System.nanoTime();
            }

            double actual_throughput = (cnt - this.executor.op.getEmpty()) * 1E6 / (end_emit - start_emit);

            if (TopologyContext.plan.getSP() != null) {
                expected_throughput += executor.getExpectedProcessRate(Constants.DEFAULT_STREAM_ID, TopologyContext.plan.getSP(), false) * 1E6;
            } else {
                expected_throughput = actual_throughput;
            }

            LOG.info(this.executor.getOP_full()
                            + "\tfinished execution and exit with throughput (k event/s) of:\t"
                            + actual_throughput + "(" + actual_throughput / expected_throughput + ")"
                            + " on node: " + node
//					+ " ( " + Arrays.show(cpu) + ")"
            );
//            LOG.info("== Spout Busy time: " + busy_time + "\t Sleep time: " + sleep_time +" ==");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                //e.printStackTrace();
            }
        }
    }

    private void emit_withControl() throws InterruptedException {
        long emitStartTime = System.currentTimeMillis();
        sp.bulk_emit(elements);
        cnt += elements;
        // Sleep for the rest of timeslice if needed
        long emitTime = System.currentTimeMillis() - emitStartTime;
        if (emitTime < timeSliceLengthMs) {// in terms of milliseconds.
            try {
                Thread.sleep(timeSliceLengthMs - emitTime);
            } catch (InterruptedException ignored) {
                //  e.printStackTrace();
            }
            sleep_time++;
        } else
            busy_time++;
    }

    /**
     * Given a desired load figure out how many elements to generate in each timeslice
     * before yielding for the rest of that timeslice
     */
    private int loadPerTimeslice() {
        return loadTargetHz / (1000 / timeSliceLengthMs); //@Notes(Tony): make each spout thread independent
    }
}
