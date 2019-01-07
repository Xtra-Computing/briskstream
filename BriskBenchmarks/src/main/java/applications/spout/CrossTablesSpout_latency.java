package applications.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.enable_admission_control;
import static engine.content.Content.CCOption_SStore;
import static engine.content.Content.CCOption_TStream;
import static utils.PartitionHelper.key_to_partition;

public class CrossTablesSpout_latency extends CrossTablesSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBenchmarkSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;


    @Override
    public void nextTuple() throws InterruptedException {
        if (ccOption == CCOption_SStore) {

//            boolean flag = read_decision[i];
            //This ensures a balanced workload among executors.
//            cnt++;
//            if (cnt % total_children_tasks == 0) {
//                i++;
//                if (i == 8)
//                    i = 0;
//            }


            int p = key_to_partition(p_generator.next());//randomly pick a starting point.

            collector.emit_single(p_bid.clone(), p, bid, number_partitions, System.nanoTime());//combined R/W executor.

            for (int k = 0; k < number_partitions; k++) {
                p_bid[p]++;
                p++;
                if (p == tthread)
                    p = 0;
            }


        } else {
//            boolean flag = read_decision[i];
//            //This ensures a balanced workload among executors.
//            cnt++;
//            if (cnt % total_children_tasks == 0) {
//                i++;
//                if (i == 8)
//                    i = 0;
//            }

            if (ccOption == CCOption_TStream) {
                if (enable_admission_control) {
                    if (control < target_Hz) {
                        collector.emit_single(bid, System.nanoTime());//combined R/W executor.
                        bid++;
                        control++;
                    } else
                        empty++;
                } else {
                    collector.emit_single(bid, System.nanoTime());//combined R/W executor.
                    bid++;
                }
                forward_checkpoint(-1, bid, null); // This is required by T-Stream.
            } else {
                collector.emit_single(bid, System.nanoTime());//combined R/W executor.

            }
        }

        bid++;
    }


}