package applications.general.sink.verbose;

import applications.general.sink.BaseSink;
import applications.general.sink.helper.stable_sink_helper_verbose;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.general.spout.helper.Event.null_expression;
import static applications.general.spout.helper.Event.split_expression;

/**
 * @author mayconbordin
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    stable_sink_helper_verbose helper;
    int processed = 0;
    private long end;
    private long start;

    public void initialize() {
        super.initialize();
        helper = new stable_sink_helper_verbose(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"),context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input) {
        final String flag = input.getString(3);
        if (flag.equals(null_expression)) {
            if (helper.execute(null, false))
                killTopology();
        } else {
            final long receive = System.nanoTime();
            final long event_time = input.getLong(0);
            final String state = "event_time" + split_expression + event_time + split_expression + flag + split_expression + "sink" + split_expression + receive;
            if (helper.execute(state, true))
                killTopology();
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
