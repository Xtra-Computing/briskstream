package brisk.topology;

import applications.util.Configuration;
import brisk.components.Topology;
import brisk.components.TopologyComponent;
import brisk.execution.ExecutionGraph;
import brisk.optimization.OptimizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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


        //launch
        OM = new OptimizationManager(g, conf, conf.getBoolean("profile", false),
                conf.getDouble("relax", 1), topology.getPlatform());//support different kinds of optimization module.
        OM.lanuch(topology, topology.getPlatform());
        OM.start();
        return g.topology;
    }
}
