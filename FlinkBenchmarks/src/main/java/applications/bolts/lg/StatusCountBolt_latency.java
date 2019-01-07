package applications.bolts.lg;


import applications.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.constants.LogProcessingConstants.Field;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt_latency extends AbstractBolt {
	private static final Logger LOG = LoggerFactory.getLogger(StatusCountBolt_latency.class);
	private Map<Integer, Integer> counts;

	@Override
	public void initialize() {
		this.counts = new HashMap<>();
		LOG.info(Thread.currentThread().getName());
	}

	@Override
	public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
		int statusCode = input.getIntegerByField(Field.RESPONSE);
		int count = 0;
		Long msgId;
		Long SYSStamp;

		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

		if (counts.containsKey(statusCode)) {
			count = counts.get(statusCode);
		}

		count++;
		counts.put(statusCode, count);

		collector.emit(new Values(statusCode, count, msgId, SYSStamp));
//        if (stat != null) stat.end_measure();
	}

	@Override
	public Fields getDefaultFields() {
		return new Fields(Field.RESPONSE, Field.COUNT, MSG_ID, SYSTEMTIMESTAMP);
	}
}
