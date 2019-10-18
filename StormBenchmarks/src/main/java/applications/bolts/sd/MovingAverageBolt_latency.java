package applications.bolts.sd;


import applications.bolts.AbstractBolt;
import constants.SpikeDetectionConstants;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static applications.Constants.Marker_STREAM_ID;
import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class MovingAverageBolt_latency extends AbstractBolt {
    //    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageBolt.class);
//    int loop = 1;
//    int cnt = 0;
    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;

    @Override
    public void initialize() {
        movingAverageWindow = config.getInt(SpikeDetectionConstants.Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {

        String deviceID = input.getStringByField(SpikeDetectionConstants.Field.DEVICE_ID);
        double nextDouble = input.getDoubleByField(SpikeDetectionConstants.Field.VALUE);
        double movingAvergeInstant = movingAverage(deviceID, nextDouble);

        if (input.getSourceStreamId().equalsIgnoreCase(Marker_STREAM_ID)) {
            collector.emit(Marker_STREAM_ID, new Values(deviceID, movingAvergeInstant
                    , nextDouble, input.getLongByField(MSG_ID), input.getLongByField(SYSTEMTIMESTAMP)));
        } else {
            collector.emit(new Values(deviceID, movingAvergeInstant, nextDouble));
        }

    }

    public double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<>();
        double sum = 0.0;

        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow - 1) {
                double valueToRemove = valueList.removeFirst();
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum / valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }

    @Override
    public Fields getDefaultFields() {
        this.fields.put(Marker_STREAM_ID,
                new Fields(SpikeDetectionConstants.Field.DEVICE_ID,
                        SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE, MSG_ID, SYSTEMTIMESTAMP));

        return new Fields(SpikeDetectionConstants.Field.DEVICE_ID,
                SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE);
    }
}