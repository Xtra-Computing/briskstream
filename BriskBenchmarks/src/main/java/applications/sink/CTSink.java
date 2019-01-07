package applications.sink;

import applications.bolts.ct.TransactionResult;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(CTSink.class);
    private static final long serialVersionUID = 5481794109405775823L;


    double success = 0;
    double failure = 0;

    @Override
    public void execute(Tuple input) {


        TransactionResult result = (TransactionResult) input.getValue(0);
        if (result.isSuccess()) {
            success++;
        } else
            failure++;

        double results;
        results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                check();
            }
        }
    }


    public void display() {
        LOG.info("Success: " + success + "(" + (success / (success + failure)) + ")");
        LOG.info("Failure: " + failure + "(" + (failure / (success + failure)) + ")");
    }
}
