package applications.bolts.sd;


import applications.bolts.AbstractBolt;
import constants.SpikeDetectionConstants;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class SpikeDetectionBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionBolt.class);
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
//        if (stat != null) stat.start_measure();
//        if (cnt < queue_size) {//make sure no gc due to queue full.
//        cnt++;
        String deviceID = input.getStringByField(SpikeDetectionConstants.Field.DEVICE_ID);
        double movingAverageInstant = input.getDoubleByField(SpikeDetectionConstants.Field.MOVING_AVG);
        double nextDouble = input.getDoubleByField(SpikeDetectionConstants.Field.VALUE);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            collector.emit(new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
//            cnt1++;
        }
        //      double v = (cnt - cnt1) / cnt;
//        }
//        if (stat != null) stat.end_measure();
    }

    public void display() {
//        LOG.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(SpikeDetectionConstants.Field.DEVICE_ID, SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE, SpikeDetectionConstants.Field.MESSAGE);
    }
}