package applications.sink;

import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.util.SINK_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.NUM_EVENTS;
import static applications.CONTROL.combo_bid_size;

public class SINKComBO extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(SINKComBO.class);
    private static final long serialVersionUID = 5481794109405775823L;

    int cnt = 0;

    boolean start_measure = false;

    @Override
    public void execute(Tuple input) {
        cnt++;
        if (cnt == combo_bid_size) {
            cnt = 0;//clear.
            int global_cnt = SINK_CONTROL.getInstance().GetAndUpdate();

            if (thisTaskId == 0) {//the first thread.
                if (!start_measure) {//only once.
                    helper.StartMeasurement();
                    start_measure = true;
                } else if (global_cnt >= (NUM_EVENTS - 40 * combo_bid_size)) {
                    double results = helper.EndMeasurement(global_cnt);
//                    LOG.info("Received:" + global_cnt + " throughput:" + results);
                    SINK_CONTROL.getInstance().throughput = results;
                    measure_end();
                }
            }
        }
    }

    public void display() {
    }
}
