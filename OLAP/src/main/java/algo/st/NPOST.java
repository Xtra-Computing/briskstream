package algo.st;

import algo.JoinAlgo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import struct.NPJ_typles;
import struct.Relation_t;
import struct.Result_t;
import struct.Tuple_t;

import static parameters.NPJ_params.*;
import static profilier.MeasureTools.*;
import static struct.NPJ_typles.HASH;

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

        END_TIME_MEASURE_NO_PAT(0);//only one thread.

        /* now print the timing results: */
        PRINT_TIMING_NO_PAT(LOG, 0, result);


        joinresult.totalresults = result;
        joinresult.nthreads = 1;
        return joinresult;
    }


    private long probe_hashtable(NPJ_typles.Hashtable_t ht, Relation_t relS) {
        int i, j;
        long matches;

        final int hashmask = ht.hash_mask;
        final int skipbits = ht.skip_bits;

        int prefetch_index = 0;
        if (PREFETCH_NPJ) {
            prefetch_index = PREFETCH_DISTANCE;
        }

        matches = 0;

        for (i = 0; i < relS.num_tuples; i++) {

            if (PREFETCH_NPJ) {
                if (prefetch_index < relS.num_tuples) {
                    int idx_prefetch = HASH(relS.tuples[prefetch_index++].key, hashmask, skipbits);
//                    __builtin_prefetch(ht.buckets + idex_prefetch, 0, 1); // TODO: need to patch JDK to support this. See https://stackoverflow.com/questions/22689712/prefetch-instruction-in-jvm-java
//                    Unsafe.prefetchRead(ht.buckets + idex_prefetch,0 ,1); //
                }
            }

            int idx = HASH(relS.tuples[prefetch_index++].key, hashmask, skipbits);
            NPJ_typles.Bucket_t bucket = ht.buckets[idx];

            do {
                for (j = 0; j < bucket.count; j++) {
                    if (relS.tuples[i].key == bucket.tuples[j].key) {
                        matches++;
                    }
                }
                bucket = bucket.next;
            } while (bucket != null);
        }

        return matches;
    }

    /**
     * Single-thread hashtable build method, ht is pre-allocated.
     *
     * @param ht   hastable to be built
     * @param relR the build relation
     */
    private void build_hashtable_st(NPJ_typles.Hashtable_t ht, Relation_t relR) {

        int i;

        final int hashmask = ht.hash_mask;
        final int skipbits = ht.skip_bits;

        for (i = 0; i < relR.num_tuples; i++) {
            Tuple_t dest;
            NPJ_typles.Bucket_t curr, nxt;
            int idx = HASH(relR.tuples[i].key, hashmask, skipbits);

            /* copy the tuple to appropriate hash bucket */
            /* if full, follow nxt pointer to find correct place */
            curr = ht.buckets[idx];
            nxt = curr.next;

            if (curr.count == BUCKET_SIZE) {
                if (nxt != null || nxt.count == BUCKET_SIZE) {
                    NPJ_typles.Bucket_t b = new NPJ_typles.Bucket_t();
                    curr.next = b;
                    b.next = nxt;
                    b.count = 1;
                    dest = b.tuples[0];
                } else {
                    dest = nxt.tuples[nxt.count];
                    nxt.count++;
                }
            } else {
                dest = curr.tuples[curr.count];
                curr.count++;
            }
            dest = relR.tuples[i];
        }

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
