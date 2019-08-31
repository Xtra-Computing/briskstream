package applications.bolts.sd;


import applications.bolts.AbstractBolt;
import applications.constants.SpikeDetectionConstants;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.Marker_STREAM_ID;
import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class SpikeDetectionBolt_latency extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionBolt_latency.class);
    double cnt = 0;
    double cnt1 = 0;
    int loop = 1;
    private double spikeThreshold;


    @Override
    public void initialize() {
        spikeThreshold = config.getDouble(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public void execute(Tuple input) {
//
        String deviceID = input.getStringByField(SpikeDetectionConstants.Field.DEVICE_ID);
        double movingAverageInstant = input.getDoubleByField(SpikeDetectionConstants.Field.MOVING_AVG);
        double nextDouble = input.getDoubleByField(SpikeDetectionConstants.Field.VALUE);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {

            if (input.getSourceStreamId().equalsIgnoreCase(Marker_STREAM_ID)) {
                collector.emit(Marker_STREAM_ID, new Values(
                        deviceID, movingAverageInstant, nextDouble,
                        "spike detected", input.getLongByField(MSG_ID), input.getLongByField(SYSTEMTIMESTAMP)));
            }
            collector.emit(new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
        }
    }

    public void display() {
//        LOG.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public Fields getDefaultFields() {
        this.fields.put(Marker_STREAM_ID, new Fields(SpikeDetectionConstants.Field.DEVICE_ID, SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE, SpikeDetectionConstants.Field.MESSAGE, MSG_ID, SYSTEMTIMESTAMP));

        return new Fields(SpikeDetectionConstants.Field.DEVICE_ID, SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE, SpikeDetectionConstants.Field.MESSAGE);
    }
}