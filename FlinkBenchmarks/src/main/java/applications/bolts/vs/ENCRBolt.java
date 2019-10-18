package applications.bolts.vs;


import applications.bolts.comm.AbstractFilterBolt;
import model.cdr.CallDetailRecord;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.VoIPSTREAMConstants.Field;

/**
 * Per-user new callee rate
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ENCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ENCRBolt.class);


    public ENCRBolt() {
        super("encr", Field.RATE, null);
    }

    @Override
    public void execute(Tuple input) {

        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        boolean newCallee = input.getBooleanByField(Field.NEW_CALLEE);

        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(Field.CALLING_NUM);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);

            collector.emit(new Values(caller, timestamp, rate, cdr));
        }

    }


}