package brisk.optimization.impl.scheduling;

import applications.util.Configuration;
import brisk.execution.ExecutionGraph;
import brisk.execution.ExecutionNode;
import brisk.optimization.impl.SchedulingPlan;
import brisk.optimization.model.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by tony on 7/11/2017.
 */
class randomSearch_NoConstrains extends randomSearch_Constraints {
    private final static Logger LOG = LoggerFactory.getLogger(randomSearch_NoConstrains.class);

    randomSearch_NoConstrains(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        super(graph, numNodes, numCPUs, cons, conf);
    }

    SchedulingPlan Packing(SchedulingPlan sp, ExecutionGraph graph, ArrayList<ExecutionNode> sort_opList) {

        final Iterator<ExecutionNode> iterator = sort_opList.iterator();
        Random r = new Random();
        while (iterator.hasNext()) {
            ExecutionNode executor = iterator.next();
            sp.allocate(executor, r.nextInt(numNodes));
        }
        sp.set_success();
        return sp;
    }
}