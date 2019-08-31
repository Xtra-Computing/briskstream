package brisk.components.operators.executor;

import brisk.components.operators.api.Checkpointable;
import brisk.components.operators.api.Operator;
import brisk.execution.Clock;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.faulttolerance.Writer;

public abstract class SpoutExecutor implements IExecutor {
    private static final long serialVersionUID = -6394372792803974178L;
    private final Operator op;

    SpoutExecutor(Operator op) {
        this.op = op;
    }

    public void setExecutionNode(ExecutionNode e) {

        op.setExecutionNode(e);
    }

    public void configureWriter(Writer writer) {
        if (op.state != null) {
            op.state.writer = writer;
        }
    }
    public int getStage() {
        return op.getFid();
    }

    @Override
    public void clean_state(Marker marker) {
        ((Checkpointable) op).ack_checkpoint(marker);
    }

    @Override
    public void earlier_clean_state(Marker marker) {
        ((Checkpointable) op).earlier_ack_checkpoint(marker);
    }

    public boolean IsStateful() {
        return op.IsStateful();
    }

    public void forceStop() {
        op.forceStop();
    }


    public void setclock(Clock clock) {
        this.op.clock = clock;
    }


    public double getEmpty() {
        return op.getEmpty();
    }
}