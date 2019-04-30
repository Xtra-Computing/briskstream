package applications.sink;

import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.util.SINK_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.*;

public class MBSinkCombo extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(MBSinkCombo.class);
    private static final long serialVersionUID = 5481794109405775823L;

    int cnt = 0;

    @Override
    public void execute(Tuple input) {
        cnt++;
        if (cnt == combo_bid_size) {
            cnt = 0;//clear.

            int global_cnt = SINK_CONTROL.getInstance().GetAndUpdate();

            if (global_cnt == combo_bid_size) {//only once.
                helper.StartMeasurement();
            } else if (global_cnt >= (NUM_EVENTS - 40 * combo_bid_size)) {
                double results = helper.EndMeasurement(cnt);
                this.setResults(results);
                if (!enable_engine)//performance measure for TStream is different.
                    LOG.info("Received:" + global_cnt + " throughput:" + results);

                if (thisTaskId == 0)//the first thread.
                    measure_end();

            }

        }
    }

    public void display() {
    }
}
