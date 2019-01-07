package applications.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.CONTROL.enable_admission_control;
import static engine.content.Content.CCOption_SStore;
import static engine.content.Content.CCOption_TStream;
import static utils.PartitionHelper.key_to_partition;

public class MicroBenchmarkSpout_latency extends MicroBenchmarkSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBenchmarkSpout_latency.class);
    private static final long serialVersionUID = -2394340130331865581L;

    @Override
    public void nextTuple() throws InterruptedException {

        if (ccOption == CCOption_SStore) {

            boolean flag2 = multi_partion_decision[j];
            j++;
            if (j == 8)
                j = 0;

//            //LOG.DEBUG("Sending out PID: " + p + ", p_bid: " + Arrays.toString(p_bid));

            if (flag2) {//multi-partition

                int p = key_to_partition(p_generator.next());//randomly pick a starting point.

                collector.emit_single(p_bid.clone(), p, bid, number_partitions, System.nanoTime());//combined R/W executor.

                for (int k = 0; k < number_partitions; k++) {
                    p_bid[p]++;
                    p++;
                    if (p == tthread)
                        p = 0;
                }

            } else {//single

                collector.emit_single(p_bid.clone(), p, bid, 1, System.nanoTime());//combined R/W executor.
                p_bid[p]++;
                p++;
                if (p == tthread)
                    p = 0;
            }


        } else {

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