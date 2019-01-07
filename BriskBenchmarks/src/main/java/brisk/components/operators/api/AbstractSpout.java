package brisk.components.operators.api;

import brisk.execution.runtime.tuple.impl.Marker;
import org.slf4j.Logger;

/**
 * Abstract AbstractSpout is a special partition-pass Operator.
 */
public abstract class AbstractSpout extends Operator {

    private static final long serialVersionUID = -7455539617930687503L;


    //the following are used for checkpoint
    protected int myiteration = 0;//start from 1st iteration.
    protected boolean success = true;
    protected long boardcast_time;


    protected AbstractSpout(Logger log) {
        super(log, true, -1, 1);
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public abstract void nextTuple() throws InterruptedException;

    public void nextTuple_nonblocking() throws InterruptedException {
        nextTuple();
    }


    /**
     * When all my consumers callback_bolt, I can  delete source message.
     *
     * @param callee
     * @param marker
     */
    public void callback(int callee, Marker marker) {
        state.callback_spout(callee, marker, executor);
    }
}
