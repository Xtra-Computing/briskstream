package applications.sink;

import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SINK_CONTROL;

import static applications.CONTROL.NUM_EVENTS;
import static applications.CONTROL.sink_combo_bid_size;

public class SINKCombo extends MeasureSink {
    private static final Logger LOG = LoggerFactory.getLogger(SINKCombo.class);
    private static final long serialVersionUID = 5481794109405775823L;
    int cnt = 0;
    boolean start_measure = false;


    public void start(){
        if (!start_measure) {//only once.
            helper.StartMeasurement();
            start_measure = true;
        }
    }


    @Override
    public void execute(Tuple input) throws InterruptedException {
        cnt++;



        if (cnt == sink_combo_bid_size) {
            cnt = 0;//clear.
            int global_cnt = SINK_CONTROL.getInstance().GetAndUpdate();

//            if (enable_debug) {
//            LOG.info("global_cnt:" + global_cnt);
//            }

            if (global_cnt >= (NUM_EVENTS - tthread * sink_combo_bid_size)) {
                double results = helper.EndMeasurement(global_cnt);
//                    LOG.info("Received:" + global_cnt + " throughput:" + results);
                measure_end(results);
                throw new InterruptedException();
            }
        } else {//left over

        }

    }

    public void display() {
    }
}
