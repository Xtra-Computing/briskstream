package applications.bolts.wc;

import applications.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.constants.WordCountConstants.Field;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static applications.constants.BaseConstants.BaseField.MSG_ID;


public class WordCountBolt_latency extends AbstractBolt {
	private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_latency.class);
	private final Map<String, MutableLong> counts = new HashMap<>();

	@Override
	public Fields getDefaultFields() {


		return new Fields(Field.WORD, Field.COUNT, MSG_ID, Field.SYSTEMTIMESTAMP);
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField(Field.WORD);
		MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
		count.increment();

		Long msgId;
		Long SYSStamp;
		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);
		Values objects = new Values(word, count.longValue(), msgId, SYSStamp);
		collector.emit(objects);

	}
}
