package brisk.optimization.routing;

import brisk.controller.output.OutputController;
import brisk.controller.output.PartitionController;
import brisk.execution.ExecutionGraph;
import brisk.execution.ExecutionNode;
import brisk.optimization.impl.SchedulingPlan;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Created by I309939 on 11/8/2016.
 */
public class RoutingPlan {
    final SchedulingPlan schedulingPlan;
    private final ExecutionGraph graph;
    private final LinkedHashMap<Integer, OutputController> plan = new LinkedHashMap<>();

    public RoutingPlan(ExecutionGraph graph, SchedulingPlan schedulingPlan) {
        this.graph = graph;
        this.schedulingPlan = schedulingPlan;
        loadingPlan(graph);
    }

    public void updateExtendedTargetId() {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (e.isLeafNode()) continue;
            Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
            for (String streamId : s)
                for (PartitionController PC : e.getController().getPartitionController(streamId)) {
                    PC.updateExtendedTargetId();
                }

        }

    }

    private void loadingPlan(ExecutionGraph graph) {
        for (ExecutionNode executionNode : graph.getExecutionNodeArrayList()) {
            int id = executionNode.getExecutorID();
            OutputController controller = executionNode.getController();
            plan.put(id, controller);
        }
    }

}
