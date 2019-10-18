package applications.bolts.wc;

import applications.bolts.AbstractBolt;
import constants.WordCountConstants.Field;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class WordCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
    private final Map<String, MutableLong> counts = new HashMap<>();

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }//, MSG_ID, Field.SYSTEMTIMESTAMP enable for latency measurements

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField(Field.WORD);
        MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
        count.increment();
//		Values objects = new Values(word, count.longValue(), input.getLongByField(MSG_ID), input.getLongByField(Field.SYSTEMTIMESTAMP)); enable for latency measurements
        Values objects = new Values(word, count.longValue());
        collector.emit(objects);
    }
}
