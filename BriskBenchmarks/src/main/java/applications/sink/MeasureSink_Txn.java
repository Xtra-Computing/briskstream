package applications.sink;

import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasureSink_Txn extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink_Txn.class);
    private static final long serialVersionUID = 5481794109405775823L;


    @Override
    public void execute(Tuple input) {
        double results;
        results = helper.execute(input.getBID());
        if (results != 0) {
            this.setResults(results);
            LOG.info("Sink finished:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end();
            }
        }
    }


}
