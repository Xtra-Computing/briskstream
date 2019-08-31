package applications.bolts.vs;


import applications.bolts.comm.AbstractScoreBolt;
import applications.model.cdr.CallDetailRecord;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FoFiRBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FoFiRBolt.class);

    public FoFiRBolt() {
        super("fofir");
    }

    @Override
    public void execute(Tuple input) {

        CallDetailRecord cdr = (CallDetailRecord) input.getValue(3);
        String number = input.getString(0);
        long timestamp = input.getLong(1);
        double rate = input.getDouble(2);

        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId(input.getSourceComponent());

        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);

            if (e.isFull()) {

                double ratio = (e.get(Source.ECR) / e.get(Source.RCR));
                double score = score(thresholdMin, thresholdMax, ratio);

                collector.emit(new Values(number, timestamp, score, cdr));
                map.remove(key);
            } else {
                //LOG.DEBUG(String.format("Inconsistent entry: source=%s; %s",
//						input.getSourceComponent(), e.toString()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }

    }


    @Override
    protected Source[] getFields() {
        return new Source[]{Source.RCR, Source.ECR};
    }
}