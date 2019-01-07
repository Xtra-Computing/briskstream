package applications.sink;

import applications.bolts.ct.TransactionResult;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.enable_latency_measurement;

public class CTSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(CTSink.class);
    private static final long serialVersionUID = 5481794109405775823L;


    double success = 0;
    double failure = 0;
    int cnt = 0;

    @Override
    public void execute(Tuple input) {

        if (input.getValue(0) instanceof TransactionResult) {
            TransactionResult result = (TransactionResult) input.getValue(0);
            if (result.isSuccess()) {
                success++;
            } else
                failure++;
        }
        check(cnt);
        cnt++;
        if (enable_latency_measurement)
            if (isSINK) {// && cnt % 1E3 == 0
                long msgId = input.getBID();
                if (msgId < max_num_msg) {
                    final long end = System.nanoTime();
                    final long start = input.getLong(1);
                    final long process_latency = end - start;//ns
//				final Long stored_process_latency = latency_map.getOrDefault(msgId, 0L);
//				if (process_latency > stored_process_latency)//pick the worst.
//				{
//				LOG.debug("msgID:" + msgId + " is at:\t" + process_latency / 1E6 + "\tms");
                    latency_map.put(msgId, process_latency);
//				}
//                    num_msg++;
                }
            }
    }


    public void display() {
        LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
        LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
    }
}
