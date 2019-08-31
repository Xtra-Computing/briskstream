package applications.sink;


import applications.sink.helper.stable_sink_helper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * @author mayconbordin
 */
public class ForwardSink_latency extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardSink_latency.class);
    stable_sink_helper helper;

    @Override
    public void initialize() {

        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , config.getString("metrics.output"), config.getDouble("predict", 0), 0, context.getThisTaskId());
    }

    @Override
    public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
        collector.emit(new Values(input, input.getLongByField(MSG_ID), input.getLongByField(SYSTEMTIMESTAMP)));
//        if (stat != null) stat.end_measure();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public Fields getDefaultFields() {

        return new Fields("", MSG_ID, SYSTEMTIMESTAMP);
    }

}
