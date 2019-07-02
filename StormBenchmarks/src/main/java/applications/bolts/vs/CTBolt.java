package applications.bolts.vs;

import applications.bolts.comm.AbstractFilterBolt;
import applications.model.cdr.CallDetailRecord;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user total call time
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
	private static final Logger LOG = LoggerFactory.getLogger(CTBolt.class);


	public CTBolt(String configPrefix) {
		super(configPrefix, Field.RATE, null);
	}

	@Override
	public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
		CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
		boolean newCallee = input.getBooleanByField(Field.NEW_CALLEE);
//        cnt++;
		if (cdr.isCallEstablished() && newCallee) {
			String caller = input.getStringByField(Field.CALLING_NUM);
			long timestamp = cdr.getAnswerTime().getMillis() / 1000;

			filter.add(caller, cdr.getCallDuration(), timestamp);
			double calltime = filter.estimateCount(caller, timestamp);
//            cnt1++;
			//LOG.DEBUG(String.format("CallTime: %f", calltime));
			collector.emit(new Values(caller, timestamp, calltime, cdr));
		}
//        double i = cnt1 / cnt;
//        if (stat != null) stat.end_measure();
	}

	public void display() {
//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
	}
}