package applications.sink;

import applications.bolts.ct.TransactionResult;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.num_events;

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
        if (cnt == 0) {
            helper.StartMeasurement();
        } else if (cnt == num_events - 1) {
            double results = helper.EndMeasurement(cnt);
            this.setResults(results);
            LOG.info("Received:" + cnt + " throughput:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                check();
            }

        }
        cnt++;
    }


    public void display() {
        LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
        LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
    }
}
