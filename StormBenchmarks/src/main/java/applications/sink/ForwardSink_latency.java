package applications.general.sink;


import applications.general.sink.helper.stable_sink_helper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.Marker_STREAM_ID;
import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * @author mayconbordin
 */
public class ForwardSink_latency extends BaseSink {
	private static final Logger LOG = LoggerFactory.getLogger(ForwardSink_latency.class);
	stable_sink_helper helper;

	public ForwardSink_latency() {
		super();
	}

	@Override
	public void initialize() {

		helper = new stable_sink_helper(LOG
				, config.getInt("runtimeInSeconds")
				, config.getString("metrics.output"), config.getDouble("predict", 0), 0, context.getThisTaskId());
	}

	@Override
	public void execute(Tuple input) {

		if (input.getSourceStreamId().equalsIgnoreCase(Marker_STREAM_ID)) {
			collector.emit(Marker_STREAM_ID, new Values(input, input.getLongByField(MSG_ID), input.getLongByField(SYSTEMTIMESTAMP)));
		} else {
			collector.emit(new Values(input));
		}

	}

	@Override
	protected Logger getLogger() {
		return LOG;
	}


	@Override
	public Fields getDefaultFields() {

		this.fields.put(Marker_STREAM_ID, new Fields("", MSG_ID, SYSTEMTIMESTAMP));

		return new Fields("");
	}


}
