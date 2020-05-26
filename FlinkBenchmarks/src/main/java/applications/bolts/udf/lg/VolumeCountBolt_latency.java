package applications.bolts.udf.lg;


import applications.bolts.AbstractBolt;
import constants.BaseConstants;
import constants.LogProcessingConstants.Conf;
import constants.LogProcessingConstants.Field;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * This bolt will count number of log events per minute
 */
public class VolumeCountBolt_latency extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VolumeCountBolt_latency.class);

    private CircularFifoBuffer buffer;
    private Map<Long, MutableLong> counts;


    @Override
    public void initialize() {
        int windowSize = config.getInt(Conf.VOLUME_COUNTER_WINDOW, 60);

        buffer = new CircularFifoBuffer(windowSize);
        counts = new HashMap<>(windowSize);
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
        long minute = input.getLongByField(Field.TIMESTAMP_MINUTES);
        Long msgId;
        Long SYSStamp;

        msgId = input.getLongByField(MSG_ID);
        SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

        MutableLong count = counts.get(minute);

        if (count == null) {
            if (buffer.isFull()) {
                long oldMinute = (Long) buffer.remove();
                counts.remove(oldMinute);
            }
            count = new MutableLong(1);
            counts.put(minute, count);
            buffer.add(minute);
        } else {
            count.increment();
        }

        collector.emit(new Values(minute, count.longValue(), msgId, SYSStamp));
//        if (stat != null) stat.end_measure();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP_MINUTES, Field.COUNT, MSG_ID, SYSTEMTIMESTAMP);
    }
}
