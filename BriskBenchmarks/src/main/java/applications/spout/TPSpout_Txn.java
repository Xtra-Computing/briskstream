package applications.spout;

import applications.tools.FastZipfGenerator;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.TransactionalSpout;
import brisk.execution.ExecutionGraph;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static applications.CONTROL.enable_admission_control;
import static applications.CONTROL.enable_latency_measurement;
import static engine.content.Content.CCOption_TStream;
import static engine.profiler.Metrics.NUM_ITEMS;

public class TPSpout_Txn extends TPSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TPSpout_Txn.class);
    private static final long serialVersionUID = -2394340130331865581L;

    @Override
    public void nextTuple() throws InterruptedException {
        if (ccOption == CCOption_TStream)//Dispatcher is not transactional, so only forward one wm to the first instance
            forward_checkpoint_single(-1, bid, null); // This is only required by T-Stream.

//        if (bid < NUM_EVENTS) {
            if (ccOption == CCOption_TStream && enable_admission_control) {
                control_emit();
            } else {
                if (enable_latency_measurement)
                    collector.emit_single(bid++, array_array[counter], System.nanoTime());//combined R/W executor.
                else
                    collector.emit_single(bid++, array_array[counter]);//combined R/W executor.
            }
            counter++;
            if (counter == array_array.length) {
                counter = 0;
            }
//        }
    }
}