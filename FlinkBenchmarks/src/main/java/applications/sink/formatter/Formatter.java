package applications.sink.formatter;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import util.Configuration;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Formatter {
    protected Configuration config;
    protected TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.config = config;
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
