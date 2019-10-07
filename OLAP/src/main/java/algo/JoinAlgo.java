package algo;

import org.slf4j.Logger;
import struct.Relation_t;
import struct.Result_t;

public abstract class JoinAlgo {

    private final Logger LOG;

    public JoinAlgo(Logger log) {

        LOG = log;
    }

    public void print_algo_name() {
        LOG.info(algo_name());
    }

    public abstract Result_t join(Relation_t relR, Relation_t relS, int nthreads);

    public abstract String algo_name();
}
