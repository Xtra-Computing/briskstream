package applications.bolts.lg;

import applications.bolts.AbstractBolt;
import constants.BaseConstants;
import model.geoip.IPLocation;
import model.geoip.IPLocationFactory;
import model.geoip.Location;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.BaseConstants.BaseConf;
import static constants.BaseConstants.BaseField.MSG_ID;
import static constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt_latency extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeographyBolt_latency.class);

    private IPLocation resolver;

    private double cnt = 0, cnt1 = 0;


    @Override
    public void initialize() {
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple input) {

        String ip = input.getStringByField(Field.IP);
        Location location = resolver.resolve(ip);

        Long msgId;
        Long SYSStamp;

        msgId = input.getLongByField(MSG_ID);
        SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
//            cnt1++;
            collector.emit(new Values(country, city, msgId, SYSStamp));
        }

    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY, MSG_ID, SYSTEMTIMESTAMP);
    }
}
