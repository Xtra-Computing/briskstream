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

import static applications.Constants.Marker_STREAM_ID;
import static constants.BaseConstants.BaseField.MSG_ID;


public class WordCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
    private final Map<String, MutableLong> counts = new HashMap<>();

    @Override
    public Fields getDefaultFields() {

        this.fields.put(Marker_STREAM_ID, new Fields(Field.WORD, Field.COUNT, MSG_ID, Field.SYSTEMTIMESTAMP));
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
//		long start = System.nanoTime();
        String word = input.getStringByField(Field.WORD);
        MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
        count.increment();


        Values objects = new Values(word, count.longValue());
        collector.emit(objects);

//		long end = System.nanoTime();
//		LOG.info("Split:" + (end - start));
    }
}
