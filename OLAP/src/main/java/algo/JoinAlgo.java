package algo;

import struct.Relation_t;
import struct.Result_t;

public abstract class JoinAlgo {
    public abstract void print_algo_name();

    public abstract Result_t join(Relation_t relR, Relation_t relS, int nthreads);

    public abstract String algo_name();
}
