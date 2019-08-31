package brisk.components.operators.executor;

import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.faulttolerance.Writer;

import java.io.Serializable;
import java.util.Map;

public interface IExecutor extends Serializable {
    long serialVersionUID = 8L;

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    int getID();

    double get_read_selectivity();

    Map<String, Double> get_input_selectivity();

    Map<String, Double> get_output_selectivity();

    double get_branch_selectivity();

    String getConfigPrefix();

    TopologyContext getContext();

    void display();

    double getResults();

    double getLoops();

    boolean isScalable();

    /**
     * Called when an executor is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     */
    void cleanup();

    void callback(int callee, Marker marker);

    void setExecutionNode(ExecutionNode e);

    Integer default_scale(Configuration conf);

    void configureWriter(Writer writer);


    void clean_state(Marker marker);

    int getStage();

    void earlier_clean_state(Marker marker);

    boolean IsStateful();

    void forceStop();

    double getEmpty();
}
