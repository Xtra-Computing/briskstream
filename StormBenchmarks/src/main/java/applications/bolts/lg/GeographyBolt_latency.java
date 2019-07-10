package applications.general.bolts.lg;

import applications.general.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.model.geoip.IPLocation;
import applications.model.geoip.IPLocationFactory;
import applications.model.geoip.Location;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.Marker_STREAM_ID;
import static applications.constants.BaseConstants.BaseConf;
import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
import static applications.constants.ClickAnalyticsConstants.Field;

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
//        cnt++;
//        if (stat != null) stat.start_measure();
		String ip = input.getStringByField(Field.IP);
		Location location = resolver.resolve(ip);

		if (location != null) {
			String city = location.getCity();
			String country = location.getCountryName();
//            cnt1++;
			if (input.getSourceStreamId().equalsIgnoreCase(Marker_STREAM_ID)) {
				collector.emit(Marker_STREAM_ID, new Values(country, city, input.getLongByField(MSG_ID), input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP)));
			} else {
				collector.emit(new Values(country, city));
			}
		}
//        double i=(cnt1-cnt)/cnt;
//        if (stat != null) stat.end_measure();
	}

	@Override
	public Fields getDefaultFields() {

		this.fields.put(Marker_STREAM_ID,
				new Fields(Field.COUNTRY, Field.CITY, MSG_ID, SYSTEMTIMESTAMP));

		return new Fields(Field.COUNTRY, Field.CITY);
	}
}
