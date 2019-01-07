package applications.sink;

import applications.bolts.ob.BidingResult;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.constants.OnlineBidingSystemConstants.Constant.num_events;

public class OBSink extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(OBSink.class);
    private static final long serialVersionUID = 5481794109405775823L;


    double success = 0;
    double failure = 0;

    int cnt = 0;

    @Override
    public void execute(Tuple input) {


        if (input.getValue(0) instanceof BidingResult) {
            BidingResult result = (BidingResult) input.getValue(0);
            if (result.isSuccess()) {
                success++;
            } else
                failure++;
        }
//        double results;
//        results = helper.execute(input.getBID());
//        if (results != 0) {
//            this.setResults(results);
//            LOG.info("Sink finished:" + results);
//            if (thisTaskId == graph.getSink().getExecutorID()) {
//                check();
//            }
//        }


//        long bid = input.getBID();
//        LOG.info("Finished:" + bid);
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
