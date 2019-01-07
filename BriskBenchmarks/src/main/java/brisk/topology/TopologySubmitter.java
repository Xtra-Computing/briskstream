package brisk.topology;

import applications.CONTROL;
import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.TopologyComponent;
import brisk.execution.ExecutionGraph;
import brisk.optimization.OptimizationManager;
import engine.common.SpinLock;
import engine.profiler.Metrics;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static applications.CONTROL.enable_shared_state;
import static applications.CONTROL.kMaxThreadNum;
import static brisk.controller.affinity.SequentialBinding.SequentialBindingInitilize;

/**
 * Created by shuhaozhang on 10/7/16.
 * <profiling/>
 * Build Brisk.topology -> Submit Brisk.topology -> Compile Brisk.topology -> obtain Brisk.execution Graph (Used as a global reference structure).
 * Launch thread for each executor mapping_node in the Brisk.execution graph.
 */
public class TopologySubmitter {
    private final static Logger LOG = LoggerFactory.getLogger(TopologySubmitter.class);
    private OptimizationManager OM;

    public OptimizationManager getOM() {
        return OM;
    }

    public void setOM(OptimizationManager OM) {
        this.OM = OM;
    }

    /**
     * TODO: support different configurations in TM.
     */

    public Topology submitTopology(Topology topology, Configuration conf) {
        //compile
        ExecutionGraph g = new TopologyComiler().generateEG(topology, conf);
        Collection<TopologyComponent> topologyComponents = g.topology.getRecords().values();

        if (CONTROL.enable_shared_state) {
            Metrics metrics = Metrics.getInstance();
            for (int i = 0; i < kMaxThreadNum; i++) {
                metrics.initilize(i);
            }
            Metrics.NUM_ACCESSES = conf.getInt("NUM_ACCESS");
            Metrics.NUM_ITEMS = conf.getInt("NUM_ITEMS");
            Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        }

        //launch
        OM = new OptimizationManager(g, conf, conf.getBoolean("profile", false),
                conf.getDouble("relax", 1), topology.getPlatform());//support different kinds of optimization module.

        if (enable_shared_state) {
            SequentialBindingInitilize();
            LOG.info("DB initialize starts @" + DateTime.now());
            long start = System.nanoTime();

            int tthread = conf.getInt("tthread");
            g.topology.spinlock = new SpinLock[tthread];//number of threads -- number of cores -- number of partitions.
            g.topology.tableinitilizer = topology.txnTopology.initializeDB(g.topology.spinlock); //For simplicity, assume all table shares the same partition mapping.
            long end = System.nanoTime();

            LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");
        }
        OM.lanuch(topology, topology.getPlatform(), topology.db);
        OM.start();
        return g.topology;
    }
}
