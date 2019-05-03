package applications.sink;

import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SINK_CONTROL;

import static applications.CONTROL.NUM_EVENTS;
import static applications.CONTROL.enable_debug;

public class SINKCombo extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(SINKCombo.class);
    private static final long serialVersionUID = 5481794109405775823L;

    int cnt = 0;

    boolean start_measure = false;




    @Override
    public void execute(Tuple input) {
        cnt++;

        if (cnt == _combo_bid_size) {
            cnt = 0;//clear.
            int global_cnt = SINK_CONTROL.getInstance().GetAndUpdate();

            if (enable_debug) {
                LOG.info("global_cnt:" + global_cnt);
            }

            if (!start_measure) {//only once.
                helper.StartMeasurement();
                start_measure = true;
            }

            if (global_cnt >= (NUM_EVENTS - 40 * _combo_bid_size)) {
                double results = helper.EndMeasurement(global_cnt);
//                    LOG.info("Received:" + global_cnt + " throughput:" + results);

                if (SINK_CONTROL.getInstance().throughput == 0) {
                    SINK_CONTROL.getInstance().throughput = results;
                    measure_end();
                }
            }

        }
    }

    public void display() {
    }
}
