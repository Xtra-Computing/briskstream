package algo.mt;

import algo.JoinAlgo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.Relation_t;
import struct.Result_t;

/**
 *
 */
public class PRO extends JoinAlgo {
    private static final Logger LOG = LoggerFactory.getLogger(PRO.class);

    public PRO() {
        super(LOG);
    }

    @Override
    public String algo_name() {
        return "Parallel Radix Join Optimized (Hardware-conscious algo. in paper)";
    }

    @Override
    public void print_algo_name() {
        LOG.info(algo_name());
    }

    @Override
    public Result_t join(Relation_t relR, Relation_t relS, int nthreads) {
        return null;
    }


}
