package brisk.optimization.impl.scheduling;

import brisk.execution.ExecutionGraph;
import brisk.execution.ExecutionNode;
import brisk.optimization.impl.SchedulingPlan;
import brisk.optimization.model.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by tony on 7/11/2017.
 */
public class roundrobin extends randomPlan_NoConstraints {
    private final static Logger LOG = LoggerFactory.getLogger(roundrobin.class);

    public roundrobin(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        super(graph, numNodes, numCPUs, cons, conf);
        LOG.info("Round robin based scheduling.");
    }

    SchedulingPlan Packing(SchedulingPlan sp, ExecutionGraph graph, ArrayList<ExecutionNode> sort_opList) {

        final Iterator<ExecutionNode> iterator = sort_opList.iterator();
        int s = 0;
        while (iterator.hasNext()) {
            ExecutionNode executor = iterator.next();
            sp.allocate(executor, s++ % numNodes);
        }
        sp.set_success();
        return sp;
    }
}