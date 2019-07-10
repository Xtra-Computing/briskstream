package applications.general.bolts.fd;

import applications.general.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.constants.FraudDetectionConstants;
import applications.model.learner.MarkovModelPredictor;
import applications.model.learner.ModelBasedPredictor;
import applications.model.learner.Prediction;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * @author maycon
 */
public class FraudPredictor_latencyBolt extends AbstractBolt {

	private static final Logger LOG = LoggerFactory.getLogger(FraudPredictor_latencyBolt.class);
	double sel = 0;
	double nsel = 0;
	int cnt = 0;
	int loop = 1;
	private ModelBasedPredictor predictor;

	@Override
	public void initialize() {
		String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL);

		if (strategy.equals("mm")) {
			predictor = new MarkovModelPredictor(config);
		}
	}

	@Override
	public void execute(Tuple input) {

		String entityID = input.getString(0);
		String record = input.getString(1);
		Prediction p = predictor.execute("Z2E821O6VB", "23UYUALXNS6M,LHL");//use fixed input to simulate the running.

		Long msgId;
		Long SYSStamp;

		msgId = input.getLongByField(MSG_ID);
		SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);


		// send outliers
		if (p.isOutlier()) {
			//  StableValues.create(entityID, p.getScore(), StringUtils.join(p.getStates(), ","));
//            sel++;
			collector.emit(new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), msgId, SYSStamp));
		}//else

	}

	public void display() {
//        LOG.info("cnt:" + cnt + "\tcnt1:" + sel + "(" + (sel / cnt) + ")");
	}

	@Override
	public Fields getDefaultFields() {
		return new Fields(
				FraudDetectionConstants.Field.ENTITY_ID,
				FraudDetectionConstants.Field.SCORE,
				FraudDetectionConstants.Field.STATES
				, MSG_ID, SYSTEMTIMESTAMP);
	}
}