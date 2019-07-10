package applications.general.bolts.comm;

import applications.general.bolts.AbstractBolt;
import applications.constants.TrafficMonitoringConstants;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Created by tony on 5/30/2017.
 */
public class ForwardBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardBolt.class);

    int cnt = 0;
    Random r = new Random();

    @Override
    public void execute(Tuple in) {
//        for(int i = new Random().nextInt(1000);i<100;i++){
//            cnt+=i;
//        }
//        if (stat != null) stat.start_measure();
        List<Object> values = in.getValues();

        values.add(1);
        collector.emit(new Values(values.toArray()));
//        if (stat != null) stat.end_measure();
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(TrafficMonitoringConstants.Field.VEHICLE_ID, TrafficMonitoringConstants.Field.DATE_TIME, TrafficMonitoringConstants.Field.OCCUPIED, TrafficMonitoringConstants.Field.SPEED,
                TrafficMonitoringConstants.Field.BEARING, TrafficMonitoringConstants.Field.LATITUDE, TrafficMonitoringConstants.Field.LONGITUDE, TrafficMonitoringConstants.Field.ROAD_ID);
    }
}
