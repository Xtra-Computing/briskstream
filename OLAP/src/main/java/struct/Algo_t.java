package struct;

import algo.JoinAlgo;

public class Algo_t {
    String name;//algorithm name
    JoinAlgo algo;

    public Algo_t(JoinAlgo algo) {
        this.name = name;
        this.algo = algo;
    }

    public Result_t join(Relation_t relR, Relation_t relS, int nthreads) {
        return algo.join(relR, relS, nthreads);
    }

    public String getName() {
        return this.algo.algo_name();
    }
}
