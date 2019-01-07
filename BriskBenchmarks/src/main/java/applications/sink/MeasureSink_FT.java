package applications.sink;

import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Marker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasureSink_FT extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink_FT.class);
    private static final long serialVersionUID = 5481794109405775823L;


    @Override
    public void execute(TransferTuple input) {
        double results;
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            final Marker marker = input.getMarker(i);
            if (marker != null) {
                this.collector.ack(input, marker);
                continue;
            }

            results = helper.execute(input.getBID());
            if (results != 0) {
                this.setResults(results);
                LOG.info("Sink finished:" + results);
                if (thisTaskId == graph.getSink().getExecutorID()) {
                    check();
                }
            }
        }
    }


}
