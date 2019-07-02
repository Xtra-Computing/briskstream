package applications.bolts.vs;

import applications.bolts.AbstractBolt;
import applications.model.cdr.CallDetailRecord;
import applications.util.math.VariableEWMA;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.VoIPSTREAMConstants.Conf;
import static applications.constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);

    private VariableEWMA avgCallDuration;
    private double decayFactor;


    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.AVERAGE);
    }

    @Override
    public void initialize() {
        decayFactor = config.getDouble(Conf.ACD_DECAY_FACTOR, 86400); //86400s = 24h
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        long timestamp = cdr.getAnswerTime().getMillis() / 1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
//        if (stat != null) stat.end_measure();
    }
}