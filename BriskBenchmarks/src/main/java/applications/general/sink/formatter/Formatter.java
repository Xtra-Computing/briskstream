package applications.general.sink.formatter;

import applications.util.Configuration;
import brisk.components.context.TopologyContext;
import brisk.execution.runtime.tuple.impl.Tuple;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
