package algo.st;

import algo.JoinAlgo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.NPJ_typles;
import struct.Relation_t;
import struct.Result_t;

import static parameters.NPJ_params.BUCKET_SIZE;
import static profilier.MeasureTools.BEGIN_TIME_MEASURE_NO_PAT;
import static profilier.MeasureTools.END_TIME_MEASURE_BUILD;

/**
 * Single thread non-partition hash join.
 */
public class NPOST extends JoinAlgo {
    private static final Logger LOG = LoggerFactory.getLogger(NPOST.class);

    public NPOST() {
        super(LOG);
    }

    @Override
    public String algo_name() {
        return "Single thread non-partition hash join";
    }

    @Override
    public Result_t join(Relation_t relR, Relation_t relS, int nthreads) {

        NPJ_typles.Hashtable_t ht = null;
        long result = 0;
        Result_t joinresult;

        long nbuckets = relR.num_tuples / BUCKET_SIZE;

        allocate_hashtable(ht, nbuckets);

        joinresult = new Result_t();

        BEGIN_TIME_MEASURE_NO_PAT(0);//only one thread.

        build_hashtable_st(ht, relR);

        END_TIME_MEASURE_BUILD(0); /* for build */

        result = probe_hashtable(ht, relS); /* for probe */


    }

    private long probe_hashtable(NPJ_typles.Hashtable_t ht, Relation_t relS) {


        return 0;
    }


    private void build_hashtable_st(NPJ_typles.Hashtable_t ht, Relation_t relR) {

    }

    /**
     * Allocates a hashtable of NUM_BUCKETS and inits everything to 0.
     *
     * @param ht pointer to a hashtable_t pointer
     */
    private void allocate_hashtable(NPJ_typles.Hashtable_t ht, long nbuckets) {

        ht = new NPJ_typles.Hashtable_t();

        ht.num_buckets = (int) Next_Power_Two(nbuckets);

        ht.initialize_buckets();

    }

    private long Next_Power_Two(long x) {
        x = x - 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }


}
