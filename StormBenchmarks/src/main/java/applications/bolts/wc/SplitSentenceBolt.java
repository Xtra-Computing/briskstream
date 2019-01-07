package applications.bolts.wc;

import applications.bolts.AbstractBolt;
import applications.constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static applications.Constants.Marker_STREAM_ID;
import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

public class SplitSentenceBolt extends AbstractBolt {
	private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
	protected final String splitregex = ",";
	long start = 0, end = 0;
	boolean update = false;
	int loop = 1;
	int cnt = 0;
	private int executionLatency = 0;
	private int curr = 0, precurr = 0;
	private int dummy = 0;

	public SplitSentenceBolt() {
		cnt = 0;
	}

	@Override
	public Fields getDefaultFields() {
		this.fields.put(Marker_STREAM_ID, new Fields(Field.WORD, MSG_ID, SYSTEMTIMESTAMP));
		return new Fields(Field.WORD);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		super.prepare(conf, context, collector);
	}

	@Override
	public void execute(Tuple input) {
//		long start = System.nanoTime();
		String value = input.getString(0);
		String[] words = value.split(splitregex);//up remote: 14161.599999999988, 13, 14; all local: 13271.8, 0, 15; down remote:11786.49, 0, 14.
		for (String word : words) {
			if (!StringUtils.isBlank(word)) {
				collector.emit(new Values(word));
			}
		}
//		long end = System.nanoTime();
//		LOG.info("Split:" + (end - start));
	}
}
