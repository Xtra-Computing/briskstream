package applications.sink.formatter;

import brisk.components.context.TopologyContext;
import brisk.execution.runtime.tuple.impl.Tuple;
import util.Configuration;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
