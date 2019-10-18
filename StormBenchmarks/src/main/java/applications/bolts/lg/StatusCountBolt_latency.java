package applications.bolts.lg;


import applications.bolts.AbstractBolt;
import constants.BaseConstants;
import constants.LogProcessingConstants.Field;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static applications.Constants.Marker_STREAM_ID;
import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

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

        if (counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }

        count++;
        counts.put(statusCode, count);
        if (input.getSourceStreamId().equalsIgnoreCase(Marker_STREAM_ID)) {
            collector.emit(Marker_STREAM_ID, new Values(statusCode, count, input.getLongByField(MSG_ID), input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP)));
        } else {
            collector.emit(new Values(statusCode, count));
        }
//        if (stat != null) stat.end_measure();
    }

    @Override
    public Fields getDefaultFields() {


        this.fields.put(Marker_STREAM_ID,
                new Fields(Field.RESPONSE, Field.COUNT, MSG_ID, SYSTEMTIMESTAMP));

        return new Fields(Field.RESPONSE, Field.COUNT);
    }
}
