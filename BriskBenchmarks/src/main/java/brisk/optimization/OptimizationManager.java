package brisk.optimization;

import applications.Platform;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.Topology;
import brisk.components.TopologyComponent;
import brisk.components.exception.UnhandledCaseException;
import brisk.execution.ExecutionGraph;
import brisk.execution.ExecutionManager;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.executorThread;
import brisk.optimization.impl.SchedulingPlan;
import brisk.optimization.model.BackPressure;
import brisk.optimization.routing.RoutingOptimizer;
import brisk.optimization.routing.RoutingPlan;
import engine.Database;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import static applications.CONTROL.enable_shared_state;
import static applications.Constants.MAP_Path;

/**
 * Created by I309939 on 11/8/2016.
 */
public class OptimizationManager extends executorThread {
    private final static Logger LOG = LoggerFactory.getLogger(OptimizationManager.class);
    private final Configuration conf;
    private final int end_cnt = 50;//50* 10=500 seconds per executor maximally
    private final long warmup_gaps = (long) (1 * 1E3);//1 seconds.
    private final boolean profile;
    private final RoutingOptimizer ro;
    private final String prefix;
    public int start_executor = 0;
    public int end_executor = 1;
    public ExecutionGraph g;
    private Optimizer so;
    private ExecutionPlan executionPlan;
    private ExecutionManager EM;
    public CountDownLatch latch;
    private long profiling_gaps = 10000;//10 seconds.
    private int profile_start = 0;
    private int profile_end = 1;
    private Topology topology;

    public OptimizationManager(ExecutionGraph g, Configuration conf, boolean profile, double relax, Platform p) {
        super(null, conf, null, null, 0, null, null, null);
        this.g = g;
        this.conf = conf;
        this.profile = profile;
        boolean benchmark = conf.getBoolean("benchmark", false);
        prefix = conf.getConfigPrefix();
        so = new Optimizer(g, conf.getBoolean("benchmark", false), conf, p, null);
        ro = new RoutingOptimizer(g);
    }

    public ExecutionManager getEM() {
        return EM;
    }

    private void profile_eachThread() {
        int cnt;
        LOG.info("Start to profile each thread!");
        executionPlan.profile_executor = 0;//start_executor reverse profile...
        end_executor = g.getSink().getExecutorID();// Math.min(end_executor, g.getExecutionNodeArrayList().size());//profile all threads except virtual.
        try {
            while (executionPlan.profile_executor <= end_executor) {
                executorThread thread = EM.ThreadMap.get(executionPlan.profile_executor);
                if (thread == null) {
                    LOG.info("Get a null thread at" + executionPlan.profile_executor);
                } else if (thread.executor.needsProfile()) {
                    profiling_gaps = profiling_gaps * Math.max(1, thread.executor.getParents_keySet().size());

                    thread.profiling = true;
                    while (thread.profiling) {
                        //LOG.DEBUG("Wait for profiling gaps" + profiling_gaps + "...for executor:" + executionPlan.profile_executor);
                        sleep(profiling_gaps);
                    }
                }
                //LOG.DEBUG(executionPlan.profile_executor + " finished profile, go for the next");
                executionPlan.profile_executor++;

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void read_meta() throws FileNotFoundException {
        String path = MAP_Path + OsUtils.OS_wrapper(prefix);
        File meta = new File(path.concat(OsUtils.OS_wrapper("Meta")));
        if (meta.exists()) {
            Scanner sc = new Scanner(meta);
            String[] split = sc.nextLine().split(" ");
            profile_start = Integer.parseInt(split[1]);

            split = sc.nextLine().split(" ");
            profile_end = Integer.parseInt(split[1]);


            split = sc.nextLine().split(" ");
            start_executor = Integer.parseInt(split[1]);

            split = sc.nextLine().split(" ");
            end_executor = Integer.parseInt(split[1]);

            //overwrite by command line:
            int profile_start_fromCMD = conf.getInt("profile_start", -1);
            if (profile_start_fromCMD != -1) {
                this.profile_start = profile_start_fromCMD;
            }

            int profile_end_fromCMD = conf.getInt("profile_end", -1);
            if (profile_end_fromCMD != -1) {
                this.profile_end = profile_end_fromCMD;
            }

        } else {

        }
    }

    public ExecutionPlan lanuch(Topology topology, Platform p, Database db) {
        this.topology = topology;
        final String initial_locks = AffinityLock.dumpLocks();
        AffinityLock.reset();
//		LOG.info("initial locks information:\n" + initial_locks);

        boolean nav = conf.getBoolean("NAV", true);
        boolean benchmark = conf.getBoolean("benchmark", false);
        boolean manual = conf.getBoolean("manual", false);
        boolean load = conf.getBoolean("load", false);
        boolean parallelism_tune = conf.getBoolean("parallelism_tune", false);

//		read_meta();

        if (!profile && !nav) {
            g.Loading(conf, p);
        }
        if (!load && parallelism_tune) {
            LOG.info("Parallelism tuning enabled.");
//                    if (OsUtils.isWindows())
            final SchedulingPlan scaling_plan = so.scalingPlan(conf.getBoolean("random", false));
            g = scaling_plan.graph;
            g.Loading(conf, p);//re-loading.
            so = new Optimizer(g, benchmark, conf, p, scaling_plan);
        }

        EM = new ExecutionManager(g, conf, this, db, p);

        //load only
        latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual

        try {
            boolean backPressure = conf.getBoolean("backPressure", false);

            if (load) {
                SchedulingPlan schedulingPlan = so.load_opt_plan(g.topology);
                executionPlan = new ExecutionPlan(schedulingPlan, null);
                if (backPressure) {
                    BackPressure.BP(schedulingPlan);
                }
                g = executionPlan.SP.graph;

                schedulingPlan.planToString(false, true);
                EM.distributeTasks(conf, executionPlan, latch, false, false, db, p);
            } else if (nav) {
                LOG.info("Native execution");
                executionPlan = new ExecutionPlan(null, null);
                executionPlan.setProfile();
                EM.distributeTasks(conf, executionPlan, latch, false, false, db, p);
//                return executionPlan;
            } else if (benchmark) {
                //manually load the desired benchmark plan.
                SchedulingPlan schedulingPlan = so.benchmark_plan(conf.getInt("plan"), prefix);
                executionPlan = new ExecutionPlan(schedulingPlan, null);
                EM.distributeTasks(conf, executionPlan, latch, true, false, db, p);
//                return executionPlan;
            } else if (profile) {
                LOG.info("Start profiling");
                executionPlan = new ExecutionPlan(null, null);
                executionPlan.setProfile();
                EM.distributeTasks(conf, executionPlan, latch, false, true, db, p);
//                return executionPlan;
            } else if (manual) {
                SchedulingPlan schedulingPlan = load_next_plan_toProfile();
                if (backPressure) {
                    BackPressure.BP(schedulingPlan);
                }
                //if (!simulation)
                EM.distributeTasks(conf, executionPlan, latch, false, false, db, p);
                //return executionPlan;
            } else {//produce the optimize plan

                SchedulingPlan schedulingPlan = so.optimize_plan();
                conf.put("predict", schedulingPlan.getOutput_rate(false) * 1E6);
                if (conf.getBoolean("routing")) {
                    RoutingPlan routingPlan = ro.optimize(new RoutingPlan(g, schedulingPlan));
                    executionPlan = new ExecutionPlan(schedulingPlan, routingPlan);
                } else {
                    executionPlan = new ExecutionPlan(schedulingPlan, null);
                }
                g = executionPlan.SP.graph;
                latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual

                if (conf.getBoolean("simulation", false)) {

                    LOG.info("Bounded throughput (k events/s):" + conf.getDouble("bound", 0));
                    LOG.info("predict throughput (k events/s):" + conf.getDouble("predict", 0));
                    System.exit(0);
                }
                if (!conf.getBoolean("monte", false)) {
                    EM.distributeTasks(conf, executionPlan, latch, false, false, db, p);
                }
            }
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }

        if (!profile && !nav && !conf.getBoolean("monte", false)) {
            executionPlan.SP.planToString(conf.getBoolean("monte", false), true);
            executionPlan.SP.set_success();
        }

        final String dumpLocks = AffinityLock.dumpLocks();
//		LOG.info("locks information:\n" + dumpLocks);
//        if (latch != null)
//            LOG.info("latch size:" + latch.getCount());
        return executionPlan;
    }

    private void storeSTAT() {

        for (ExecutionNode executor : g.getExecutionNodeArrayList()) {
            if (executor.needsProfile()) {
                if (executor.isSourceNode()) {
                    executor.profiling.get(-1).finishProfile();
                } else {
                    for (TopologyComponent parent : executor.getParents_keySet()) {
                        ExecutionNode src = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);//use middle one as producer..
                        executor.profiling.get(src.getExecutorID()).finishProfile();
                    }
                }
            }
        }
    }

    /**
     * DUMP:
     * <p>
     * tuple fieldSize; window fieldSize; JVM fieldSize;
     * profiling results of each executor.
     * this.tuple_size
     * +   "\t" + this.operation_cycles[0]
     * +   "\t" + this.operation_cycles[1]
     * +   "\t" + this.LLC_MISS_PS[0]
     * +   "\t" + this.LLC_MISS_PS[1]
     * +   "\t" + this.LLC_REF_PS[0]
     * +   "\t" + this.LLC_REF_PS[1];
     */
    private void storeProfileInfo() {
        int tuple_size = conf.getInt("size_tuple", 0);
        int window = conf.getInt("window", 0);
        int JVM_size = conf.getInt("JVM", 0);
        File file = new File(conf.getString("metrics.output").concat(OsUtils.OS_wrapper("DUMP.txt")));
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file, true), "utf-8"))) {
            StringBuilder sb = new StringBuilder();
//            sb.append(tuple_size).append("\t").append(window).append("\t").append(JVM_size).append("\t");
            for (ExecutionNode executionNode : g.getExecutionNodeArrayList()) {
                if (executionNode.needsProfile()) {
                    if (executionNode.isSourceNode()) {
                        sb.append("\n").append(executionNode.getOP()).append(" on src:")
                                .append("External")
                                .append("\n").append(executionNode.profiling.get(-1).toString()).append("\n");
                    } else {
                        for (TopologyComponent parent : executionNode.getParents_keySet()) {
                            ExecutionNode srcNode = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);//use middle one as producer..
                            sb.append("\n").append(executionNode.getOP()).append(" on src:")
                                    .append(srcNode.getOP())
                                    .append("\n").append(executionNode.profiling.get(srcNode.getExecutorID()).toString()).append("\n");
                        }
                    }
                }
            }
            LOG.info("=====DUMP information=======");
            LOG.info(sb.toString());
            LOG.info("=====DUMP information=======");
            writer.write(sb.toString() + "\n");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void dumpStatistics() {
        storeSTAT();
        storeProfileInfo();
        LOG.info("Dump profiling statistics finished.");

    }


    private void warmup() {
        LOG.info("Start warmup...");
        try {
            sleep(warmup_gaps);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Warmup finished.");
    }

    private SchedulingPlan load_next_plan_toProfile() {
        SchedulingPlan schedulingPlan = so.manual_plan(profile_start++, prefix);
        executionPlan = new ExecutionPlan(schedulingPlan, null);
        executionPlan.setProfile();
        return schedulingPlan;
    }

    private void run_profile() {
        while (profile_start <= profile_end) {
            load_next_plan_toProfile();
            try {
                EM.redistributeTasks(g, conf, executionPlan);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            profile_eachThread();
        }
    }

    private void dynamic_optimize() {

        boolean parallelism_tune = conf.getBoolean("parallelism_tune", false);
        while (true) {
            try {
//                    LOG.info("Wait for optimization gaps...");
                sleep(profiling_gaps);

                SchedulingPlan schedulingPlan = so.optimize_plan();
                RoutingPlan routingPlan = ro.optimize(new RoutingPlan(g, schedulingPlan));
                executionPlan = new ExecutionPlan(schedulingPlan, routingPlan);

                EM.redistributeTasks(g, conf, executionPlan);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * creates new txn and routing plan continuously.
     */
    public void run() {

        this.node = 0;


        //use this for NUMA-awareness!
        //Disabled normally.
//        cpu = EM.AC.requirePerCore(node);
//        binding();


//        SequentialBindingInitilize();
//        LOG.info("DB initialize starts @" + DateTime.now());
//        long start = System.nanoTime();
//        g.topology.spinlock = topology.txnTopology.initializeDB();
//        long end = System.nanoTime();
//
//        LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");

//        AffinityLock.reset();

        latch.countDown();          //tells others I'm ready.
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //initialize queue set.

        if (conf.getBoolean("Fault_tolerance", false) || enable_shared_state) {
            ExecutionManager.clock.start();
        }


        if (profile) {
            LOG.info("Wait for warm up phase...");
            warmup();
            LOG.info("Start to profile each thread...");
            profile_eachThread();
            //run_profile();//useful if we want to profile for more mapping cases..

            LOG.info("finished profiling.");
            executionPlan.disable_profile();
            EM.exist();//stop all execution threads.

            dumpStatistics();
            LOG.info("STAT ended, optimization manager exist");


        } else {
            if (conf.getBoolean("DO", false)) {
                dynamic_optimize();
            }
        }

        LOG.info("Optimization manager exists");
    }


    @Override
    protected void _execute_noControl() {

    }

    @Override
    protected void _execute() {

    }

    @Override
    protected void _profile() {

    }
}
