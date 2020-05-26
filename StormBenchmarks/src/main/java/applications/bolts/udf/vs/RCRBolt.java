package applications.bolts.udf.vs;

import applications.bolts.udf.comm.AbstractFilterBolt;
import constants.VoIPSTREAMConstants;
import model.cdr.CallDetailRecord;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static constants.VoIPSTREAMConstants.Field;

/**
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class RCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RCRBolt.class);

    //    private double cnt = 0, cnt1 = 0, cnt2 = 0;
    public RCRBolt() {
        super("rcr", Field.RATE, null);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);

        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            if (input.getSourceStreamId().equals(VoIPSTREAMConstants.Stream.DEFAULT)) {
                String callee = cdr.getCalledNumber();
                filter.add(callee, 1, timestamp);

            } else if (input.getSourceStreamId().equals(VoIPSTREAMConstants.Stream.BACKUP)) {
                String caller = cdr.getCallingNumber();
                double rcr = filter.estimateCount(caller, timestamp);
                collector.emit(new Values(caller, timestamp, rcr, cdr));
            }
        }
    }
}
